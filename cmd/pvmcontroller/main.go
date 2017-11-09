package main

import (
	"flag"
	"time"

	log "github.com/sirupsen/logrus"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	vmclient "github.com/kubevirt-incubator/persistentvm-addon/pkg/client/clientset/versioned"
	vminformer "github.com/kubevirt-incubator/persistentvm-addon/pkg/client/informers/externalversions"
	"github.com/kubevirt-incubator/persistentvm-addon/pkg/controller"
	"github.com/kubevirt-incubator/persistentvm-addon/pkg/signals"
)

var (
	masterURL  string
	kubeconfig string
)

func main() {
	flag.Parse()

	stopCh := signals.SetupSignalHandler()

	cfg, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfig)
	if err != nil {
		log.WithField("err", err).Fatal("Cannot create configuration")
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.WithField("err", err).Fatal("Cannot create kube client")
	}

	pvmClient, err := vmclient.NewForConfig(cfg)
	if err != nil {
		log.WithField("err", err).Fatal("Cannot create vm client")
	}

	pvmInformerFactory := vminformer.NewSharedInformerFactory(pvmClient, time.Second*30)

	controller := vmcontroller.NewController(
		kubeClient,
		pvmClient,
		pvmInformerFactory,
	)

	go pvmInformerFactory.Start(stopCh)

	if err := controller.Run(1, stopCh); err != nil {
		log.WithField("err", err).Fatal("Controller failed")
	}

	log.Info("Ending the controller.")

}

func init() {
	flag.StringVar(&masterURL, "masterURL", "", "Kubernetes cluster API url")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Kubernetes configuration file")
}
