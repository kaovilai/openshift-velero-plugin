package common

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/konveyor/openshift-velero-plugin/velero-plugins/clients"
	"github.com/openshift/client-go/route/clientset/versioned/scheme"
	routev1client "github.com/openshift/client-go/route/clientset/versioned/typed/route/v1"
	"github.com/openshift/library-go/pkg/image/reference"
	"github.com/sirupsen/logrus"
	velero "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const tmpOADPPath = "/tmp/openshift.io/velero-plugin"

func writeByteToDirPath(dirPath string, fileName string, data []byte) error {
	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		return err
	}
	file, err := os.Create(dirPath + "/" + fileName)
	if err != nil {
		return err
	}
	_, err = file.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func GetRegistryInfo(uid types.UID, major, minor int, log logrus.FieldLogger) (string, error) {
	registryInfoTmpFilePath := fmt.Sprintf("%s/%s/%s/", tmpOADPPath, uid, "registry-info")
	registryInfoTmpFileName := "registry-info.txt"
	registryInfoByte, err := os.ReadFile(registryInfoTmpFilePath + registryInfoTmpFileName)
	if err == nil {
		return string(registryInfoByte), nil
	}
	err = nil // reset err


	imageClient, err := clients.ImageClient()
	if err != nil {
		return "", err
	}
	imageStreams, err := imageClient.ImageStreams("openshift").List(context.Background(), metav1.ListOptions{})
	if err == nil && len(imageStreams.Items) > 0 {
		if value := imageStreams.Items[0].Status.DockerImageRepository; len(value) > 0 {
			ref, err := reference.Parse(value)
			if err == nil {
				log.Info("[GetRegistryInfo] value from imagestream")
				err := writeByteToDirPath(registryInfoTmpFilePath, registryInfoTmpFileName, []byte(ref.Registry))
				if err != nil {
					return "", err
				}
				return ref.Registry, nil
			}
		}
	}

	if major != 1 {
		return "", fmt.Errorf("server version %v.%v not supported. Must be 1.x", major, minor)
	}

	cClient, err := clients.CoreClient()
	if err != nil {
		return "", err
	}
	if minor < 7 {
		return "", fmt.Errorf("Kubernetes version 1.%v not supported. Must be 1.7 or greater", minor)
	} else if minor <= 11 {
		registrySvc, err := cClient.Services("default").Get(context.Background(), "docker-registry", metav1.GetOptions{})
		if err != nil {
			// Return empty registry host but no error; registry not found
			return "", nil
		}
		internalRegistry := registrySvc.Spec.ClusterIP + ":" + strconv.Itoa(int(registrySvc.Spec.Ports[0].Port))
		log.Info("[GetRegistryInfo] value from clusterIP")
		err = writeByteToDirPath(registryInfoTmpFilePath, registryInfoTmpFileName, []byte(internalRegistry))
		if err != nil {
			return "", err
		}
		return internalRegistry, nil
	} else {
		config, err := cClient.ConfigMaps("openshift-apiserver").Get(context.Background(), "config", metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		serverConfig := APIServerConfig{}
		err = json.Unmarshal([]byte(config.Data["config.yaml"]), &serverConfig)
		if err != nil {
			return "", err
		}
		internalRegistry := serverConfig.ImagePolicyConfig.InternalRegistryHostname
		if len(internalRegistry) == 0 {
			err = writeByteToDirPath(registryInfoTmpFilePath, registryInfoTmpFileName, []byte(""))
			if err != nil {
				return "", err
			}
			return "", nil
		}
		log.Info("[GetRegistryInfo] value from clusterIP")
		err = writeByteToDirPath(registryInfoTmpFilePath, registryInfoTmpFileName, []byte(internalRegistry))
		if err != nil {
			return "", err
		}
		return internalRegistry, nil
	}
}

func getMetadataAndAnnotations(item runtime.Unstructured) (metav1.Object, map[string]string, error) {
	metadata, err := meta.Accessor(item)
	if err != nil {
		return nil, nil, err
	}

	annotations := metadata.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	return metadata, annotations, nil
}

// returns major, minor versions for kube
func GetServerVersion() (int, int, error) {
	// save server version to tmp file
	serverVersionTmpFilePath := tmpOADPPath + "/server-version/"
	serverVersionTmpFileNameMajor := "major.txt"
	serverVersionTmpFileNameMinor := "minor.txt"
	majorByte, errMajorByte := os.ReadFile(serverVersionTmpFilePath + serverVersionTmpFileNameMajor)
	minorByte, errMinorByte := os.ReadFile(serverVersionTmpFilePath + serverVersionTmpFileNameMinor)
	if errMajorByte == nil && errMinorByte == nil {
		major, errMajor := strconv.Atoi(string(majorByte))
		if errMajor != nil {
			return 0, 0, errMajor
		}
		minor, errMinor := strconv.Atoi(string(minorByte))
		if errMinor != nil {
			return 0, 0, errMinor
		}
		return major, minor, nil
	}

	client, err := clients.DiscoveryClient()
	if err != nil {
		return 0, 0, err
	}
	version, err := client.ServerVersion()
	if err != nil {
		return 0, 0, err
	}

	// Attempt parsing version.Major/Minor first, fall back to parsing gitVersion
	major, err1 := strconv.Atoi(version.Major)
	minor, err2 := strconv.Atoi(strings.Trim(version.Minor, "+"))

	if err1 != nil || err2 != nil {
		// gitVersion format ("v1.11.0+d4cacc0")
		r, _ := regexp.Compile(`v[0-9]+\.[0-9]+\.`)
		valid := r.MatchString(version.GitVersion)
		if !valid {
			return 0, 0, errors.New("gitVersion does not match expected format")
		}
		majorMinorArr := strings.Split(strings.Split(version.GitVersion, "v")[1], ".")

		major, err = strconv.Atoi(majorMinorArr[0])
		if err != nil {
			return 0, 0, err
		}
		minor, err = strconv.Atoi(majorMinorArr[1])
		if err != nil {
			return 0, 0, err
		}
	}

	// save server version to tmp file
	err = writeByteToDirPath(serverVersionTmpFilePath, serverVersionTmpFileNameMajor, []byte(strconv.Itoa(major)))
	if err != nil {
		return 0, 0, err
	}
	err = writeByteToDirPath(serverVersionTmpFilePath, serverVersionTmpFileNameMinor, []byte(strconv.Itoa(minor)))
	if err != nil {
		return 0, 0, err
	}

	return major, minor, nil
}

// Takes Namesapce where the operator resides, name of the BackupStorageLocation and name of configMap as input and returns the Route of backup registry.
func getOADPRegistryRoute(uid types.UID, namespace string, location string, configMap string) (string, error) {

	registryTmpFilePath := fmt.Sprintf("%s/%s/%s/%s/%s/%s/", tmpOADPPath, uid, "registry-route",namespace, location, configMap)
	registryTmpFileName := "registry.txt"
	// retrieve registry hostname from temporary file
	tmpSpecHost, err := os.ReadFile(registryTmpFilePath + registryTmpFileName)
	if err == nil && len(tmpSpecHost) > 0 {
		return string(tmpSpecHost), nil
	}
	err = nil // reset error


	config, err := rest.InClusterConfig()
	if err != nil {
		return "cannot load in-cluster config", err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return "could not create client", err
	}
	cMap := client.CoreV1().ConfigMaps(namespace)
	mapClient, err := cMap.Get(context.Background(), configMap, metav1.GetOptions{})
	if err != nil {
		return "failed to find registry configmap", err
	}
	osClient, err := routev1client.NewForConfig(config)
	if err != nil {
		return "failed to generate route client", err
	}
	routeClient := osClient.Routes(namespace)
	route, err := routeClient.Get(context.Background(), mapClient.Data[location], metav1.GetOptions{})
	if err != nil {
		return "failed to find OADP registry route", err
	}

	// save the registry hostname to a temporary file
	err = writeByteToDirPath(registryTmpFilePath, registryTmpFileName, []byte(route.Spec.Host))
	if err != nil {
		return "", err
	}
	return route.Spec.Host, nil
}

// Takes Backup Name an Namespace where the operator resides and returns the name of the BackupStorageLocation
func getBackupStorageLocationForBackup(uid types.UID, name string, namespace string) (string, error) {
	bslTmpFilePath := fmt.Sprintf("%s/%s/%s/%s/%s/", tmpOADPPath, uid, "backup-storage-location", namespace, name)
	bslTmpFileName := "bsl.txt"
	// retrieve bsl for backup from temporary file
	bslForBackupFromTmp, err := os.ReadFile(bslTmpFilePath + bslTmpFileName)
	if err == nil && len(bslForBackupFromTmp) > 0 {
		return string(bslForBackupFromTmp), nil
	}
	err = nil // reset error

	config, err := rest.InClusterConfig()
	crdConfig := *config
	crdConfig.ContentConfig.GroupVersion = &schema.GroupVersion{Group: "velero.io", Version: "v1"}
	crdConfig.APIPath = "/apis"
	crdConfig.NegotiatedSerializer = serializer.NewCodecFactory(scheme.Scheme)
	crdConfig.UserAgent = rest.DefaultKubernetesUserAgent()
	result := velero.BackupList{}

	if err != nil {
		return "", err
	}
	client, err := rest.UnversionedRESTClientFor(&crdConfig)
	if err != nil {
		return "", err
	}

	err = client.
		Get().
		Namespace(namespace).
		Resource("backups").
		Do(context.Background()).
		Into(&result)
	if err != nil {
		return "", err
	}

	for _, element := range result.Items {
		if element.Name == name {
			// save the bsl for backup to a temporary file
			err = writeByteToDirPath(bslTmpFilePath, bslTmpFileName, []byte(element.Spec.StorageLocation))
			if err != nil {
				return "", err
			}
			return element.Spec.StorageLocation, nil
		}
	}
	return "", errors.New("BackupStorageLocation not found")
}

// fetches backup for a given backup name
func GetBackup(uid types.UID, name string, namespace string) (*velero.Backup, error) {
	// this function is only used by pod/restore.go to check for backup.Spec.DefaultVolumesToRestic. We can cache this
	backupTmpFilePath := fmt.Sprintf("%s/%s/%s/%s/%s/", tmpOADPPath, uid, "backup", namespace, name)
	backupTmpFileName := "backup.txt"
	// retrieve backup from temporary file
	backupFromTmp, err := os.ReadFile(backupTmpFilePath + backupTmpFileName)
	if err == nil && len(backupFromTmp) > 0 {
		backup := velero.Backup{}
		if err := json.Unmarshal(backupFromTmp, &backup); err != nil {
			return nil, err
		}
		return &backup, nil
	}
	err = nil // reset error

	if name == "" {
		return nil, errors.New("cannot get backup for an empty name")
	}

	if namespace == "" {
		return nil, errors.New("cannot get backup for an empty namespace")
	}

	config, err := rest.InClusterConfig()
	crdConfig := *config
	crdConfig.ContentConfig.GroupVersion = &schema.GroupVersion{Group: "velero.io", Version: "v1"}
	crdConfig.APIPath = "/apis"
	crdConfig.NegotiatedSerializer = serializer.NewCodecFactory(scheme.Scheme)
	crdConfig.UserAgent = rest.DefaultKubernetesUserAgent()
	result := velero.BackupList{}

	if err != nil {
		return nil, err
	}
	client, err := rest.UnversionedRESTClientFor(&crdConfig)
	if err != nil {
		return nil, err
	}

	err = client.
		Get().
		Namespace(namespace).
		Resource("backups").
		Do(context.Background()).
		Into(&result)
	if err != nil {
		return nil, err
	}

	for _, backup := range result.Items {
		if backup.Name == name {
			backupBytes, err := json.Marshal(backup)
			if err != nil {
				return nil, err
			}
			// save the backup to a temporary file
			err = writeByteToDirPath(backupTmpFilePath, backupTmpFileName, backupBytes)
			if err != nil {
				return nil, err
			}

			return &backup, nil
		}
	}
	return nil, errors.New("cannot find backup for the given name")
}

func StringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
