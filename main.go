package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/magiconair/properties"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

func main() {
	var kubeconfig, configmap *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	configmap = flag.String("configmap", "", "name of configmap")
	flag.Parse()

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(*configmap)
	ctx := context.TODO()
	cm, err := clientset.CoreV1().ConfigMaps("default").Get(ctx, *configmap, metav1.GetOptions{})
	if err != nil {
		panic(err.Error())
	}

	//fmt.Println(cm.Data)
	if cm.Data["spark.properties"] == "" {
		os.Exit(0)
	}

	propertyString := cm.Data["spark.properties"]
	// props := map[string]string{}


	props, err := properties.LoadString(propertyString)
	// err = yaml.Unmarshal([]byte(propertyString), &properties)
	if err != nil {
		panic(err)
	}

	// dont do this,  need to remove escaping from colons
	// scanner := bufio.NewScanner(strings.NewReader(propertyString))
	// for scanner.Scan() {
	// 	line := scanner.Text()
	// 	elements := strings.Split(line, "=")
	// 	if len(elements) == 2 {
	// 		properties[elements[0]] = elements[1]
	// 		fmt.Println( line)
	// 		fmt.Println( elements[1])
	// 	}
	// }
	// err = scanner.Err()
	// if err != nil {
	// 	panic(err)
	// }

	props.Set("spark.eventLog.dir", "s3://wave-spark-history/")
	//properties["spark.eventLog.dir"] = "s3://wave-spark-history/"
	props.Set("yeah", "no")

	// copy := cm.DeepCopy()

	var patchBuilder strings.Builder
	patchBuilder.WriteString("[{ \"op\": \"replace\", \"path\": \"/data\", \"value\": ")

	patchBuilder.WriteString("{\"spark.properties\": \"")
	for k, v := range props.Map() {
		patchBuilder.Write([]byte( k))
		patchBuilder.WriteString( "=")
		patchBuilder.Write( []byte(v))
		patchBuilder.WriteString( "\\n")
		fmt.Println(v)
	}
	patchBuilder.WriteString("\"}}]")

	// patch := patchValue{
	// 	Op:    "replace",
	// 	Path:  "/data",
	// 	Value: map[string]string{
	// 		"spark.properties": patchBuilder.String(),
	// 	},
	// }
	// patchData, err := json.Marshal(patch)

	patchString := patchBuilder.String()
	// patchString := `{"data":{"spark.properties": "`+patchBuilder.String()+`"}}`
	//patchString := `{"data":{"a":"b"}}`
	// xpatchString := `{"data":{"spark.properties": "#XXXX properties built from Kubernetes config map with name: guest-cee6c633-8674-4322-a6fa-29638f761c0a-1605291619156-driver-conf-map\n#Fri Nov 13 18:20:19 UTC 2020\nspark.files=/usr/local/bin/kernel-launchers/python/scripts/launch_ipykernel.py\nspark.driver.host=spark-1605291619502-driver-svc.guest-cee6c633-8674-4322-a6fa-29638f761c0a.svc\nspark.kubernetes.driver.label.kernel_id=cee6c633-8674-4322-a6fa-29638f761c0a\nspark.driver.blockManager.port=7079\nspark.kubernetes.executor.label.kernel_id=cee6c633-8674-4322-a6fa-29638f761c0a\nspark.kubernetes.driver.container.image=elyra/kernel-spark-py\\:2.3.0\nspark.kubernetes.pyspark.pythonVersion=3\nspark.kubernetes.python.mainAppResource=local\\:///usr/local/bin/kernel-launchers/python/scripts/launch_ipykernel.py\nspark.kubernetes.executor.container.image=elyra/kernel-spark-py\\:2.3.0\nspark.kubernetes.submitInDriver=true\nspark.submit.deployMode=cluster\nspark.kubernetes.memoryOverheadFactor=0.4\nspark.kubernetes.driver.label.component=kernel\nspark.kubernetes.submission.waitAppCompletion=false\nspark.kubernetes.driverEnv.HTTP2_DISABLE=true\nspark.kubernetes.resource.type=python\nspark.kubernetes.python.pyFiles=\nspark.kubernetes.executor.label.component=kernel\nspark.master=k8s\\://https\\://10.100.0.1\\:443\nspark.kubernetes.authenticate.driver.serviceAccountName=default\nspark.kubernetes.driver.pod.name=guest-cee6c633-8674-4322-a6fa-29638f761c0a-1605291619156-driver\nspark.kubernetes.executor.label.app=enterprise-gateway\nspark.kubernetes.namespace=guest-cee6c633-8674-4322-a6fa-29638f761c0a\nspark.app.id=spark-dc08d63ab16a45bd9eee7366094fae87\nspark.app.name=guest-cee6c633-8674-4322-a6fa-29638f761c0a\nspark.driver.port=7078\nspark.kubernetes.driver.label.app=enterprise-gateway\n"}}`
	fmt.Println(patchString)
	// fmt.Println(xpatchString)
	patchData := []byte(patchString)
	_, err = clientset.CoreV1().ConfigMaps("default").Patch(
		ctx, *configmap, types.JSONPatchType, patchData, metav1.PatchOptions{},
	)

	if err != nil {
		panic(err)
	}


}

type patchValue struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value map[string]string `json:"value"`
}
