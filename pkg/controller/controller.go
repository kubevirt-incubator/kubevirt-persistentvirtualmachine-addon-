package vmcontroller

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	vmclient "github.com/kubevirt-incubator/persistentvm-addon/pkg/client/clientset/versioned"
	vmscheme "github.com/kubevirt-incubator/persistentvm-addon/pkg/client/clientset/versioned/scheme"
	vminformer "github.com/kubevirt-incubator/persistentvm-addon/pkg/client/informers/externalversions"
	vmlister "github.com/kubevirt-incubator/persistentvm-addon/pkg/client/listers/persistentvm/v1alpha1"
)

const (
	controllerName   = "PVMController"
	deployAnnotation = "persistentVM/deploy"
)

// PVMController is the controller structure passed around
type PVMController struct {
	// client sets for CRUD operations on the cluster objects
	pvmClientSet vmclient.Interface

	// pvm listers to handle out CRD
	pvmLister vmlister.PVMLister
	pvmSynced cache.InformerSynced

	// used to queue the work so we do not overload the API
	workqueue workqueue.RateLimitingInterface

	// event recorder for the API
	recorder record.EventRecorder
}

// NewController creates new controller for monitoring the persistentVM objects
func NewController(
	kubeclientset kubernetes.Interface,
	pvmclientset vmclient.Interface,
	pvminformerfactory vminformer.SharedInformerFactory,
) *PVMController {

	// pvm custom resource informer
	pvmInformer := pvminformerfactory.Persistentvm().V1alpha1().PVMs()

	// event broadcaster
	vmscheme.AddToScheme(scheme.Scheme)
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(log.Printf)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerName})

	controller := &PVMController{
		pvmClientSet: pvmclientset,
		pvmLister:    pvmInformer.Informer(),
		pvmSynced:    pvmInformer.Informer().HasSynced,
		workqueue:    workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "PVMs"),
		recorder:     recorder,
	}

	// register handlers for add update and delete
	pvmInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			log.WithField("obj", obj).Info("Added object")
			controller.addToQueue(obj)
		},
		UpdateFunc: func(old, new interface{}) {
			log.WithField("obj", new).Info("Updated object")
			controller.addToQueue(new)
		},
		DeleteFunc: func(obj interface{}) {
			log.WithField("obj", obj).Info("Deleted object")
			controller.addToQueue(obj)
		},
	})

	return controller
}

// addToQueue processes the added object by pushing its name to the
// workQueue. It than gets processed when the time comes
func (c *PVMController) addToQueue(obj interface{}) {
	var (
		key string
		err error
	)

	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}

	c.workqueue.AddRateLimited(key)
}

// Run the controller and be happy
func (c *PVMController) Run(workers int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.workqueue.ShutDown()

	log.Info("Starting the controller")

	if ok := cache.WaitForCacheSync(stopCh, c.pvmSynced); !ok {
		return fmt.Errorf("Caches cannot be synced")
	}

	log.Info("Starting workers")
	for i := 0; i < workers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	log.Info("Controller is running")
	<-stopCh
	log.Info("Controller stopped")

	return nil
}

// run each worker and make it happy
func (c *PVMController) runWorker() {
	for c.processNextItem() {
	}
}

// processNextItem takes the item from workqueue and process it
// handles failure: leaves the item for precossing in next turn
func (c *PVMController) processNextItem() bool {

	obj, quit := c.workqueue.Get()
	if quit {
		return false
	}

	err := func(obj interface{}) error {
		defer c.workqueue.Done(obj)

		var key string
		var ok bool

		if key, ok = obj.(string); !ok {
			// invalid key, forget it and do other bussines,
			// this error is absorbed as it is not critical
			c.workqueue.Forget(obj)
			runtime.HandleError(fmt.Errorf("Expected string key, but go %#v", obj))
			return nil
		}

		if err := c.syncItem(key); err != nil {
			// the sync operation failed this is pitty
			return fmt.Errorf("Error syncing %s with %s", key, err.Error())
		}

		// all done and no error, such wow
		log.WithField("key", key).Info("Done processing")
		c.workqueue.Forget(obj)
		return nil
	}(obj)

	if err != nil {
		// well shoot this is pitty
		// tell kubernetes error happened
		runtime.HandleError(err)
		return true
	}

	// all done
	return true
}

// syncItem get the key and do batched processing on the item
// associated with it
func (c *PVMController) syncItem(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("Cannot get the right namespace and key for key: %s; %s", key, err.Error()))
		return nil
	}

	pvm, err := c.pvmLister.PVMs(namespace).Get(name)
	if err != nil {
		if errors.IsNotFound(err) {
			runtime.HandleError(fmt.Errorf("Object not found %s/%s", namespace, name))
			return nil
		}
		return err
	}

	if _, ok := pvm.Annotations[deployAnnotation]; ok {
		// lets do this fucking magic with that shit
		log.Info("Key already processed")
		c.recorder.Event(pvm, corev1.EventTypeNormal, "PVMLOG", "Success we have annotated PVM")
	}

	return nil
}
