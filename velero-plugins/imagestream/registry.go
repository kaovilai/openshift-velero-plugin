package imagestream

import (
	"context"
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"

	"github.com/konveyor/openshift-velero-plugin/velero-plugins/clients"
	"github.com/pkg/errors"

	"github.com/openshift/oadp-operator/pkg/credentials"

	oadpv1alpha1 "github.com/openshift/oadp-operator/api/v1alpha1"
	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Registry Env var keys
const (
	// AWS registry env vars
	RegistryStorageEnvVarKey                 = "REGISTRY_STORAGE"
	RegistryStorageS3AccesskeyEnvVarKey      = "REGISTRY_STORAGE_S3_ACCESSKEY"
	RegistryStorageS3BucketEnvVarKey         = "REGISTRY_STORAGE_S3_BUCKET"
	RegistryStorageS3RegionEnvVarKey         = "REGISTRY_STORAGE_S3_REGION"
	RegistryStorageS3SecretkeyEnvVarKey      = "REGISTRY_STORAGE_S3_SECRETKEY"
	RegistryStorageS3RegionendpointEnvVarKey = "REGISTRY_STORAGE_S3_REGIONENDPOINT"
	RegistryStorageS3RootdirectoryEnvVarKey  = "REGISTRY_STORAGE_S3_ROOTDIRECTORY"
	RegistryStorageS3SkipverifyEnvVarKey     = "REGISTRY_STORAGE_S3_SKIPVERIFY"
	// Azure registry env vars
	RegistryStorageAzureContainerEnvVarKey       = "REGISTRY_STORAGE_AZURE_CONTAINER"
	RegistryStorageAzureAccountnameEnvVarKey     = "REGISTRY_STORAGE_AZURE_ACCOUNTNAME"
	RegistryStorageAzureAccountkeyEnvVarKey      = "REGISTRY_STORAGE_AZURE_ACCOUNTKEY"
	RegistryStorageAzureSPNClientIDEnvVarKey     = "REGISTRY_STORAGE_AZURE_SPN_CLIENT_ID"
	RegistryStorageAzureSPNClientSecretEnvVarKey = "REGISTRY_STORAGE_AZURE_SPN_CLIENT_SECRET"
	RegistryStorageAzureSPNTenantIDEnvVarKey     = "REGISTRY_STORAGE_AZURE_SPN_TENANT_ID"
	RegistryStorageAzureAADEndpointEnvVarKey     = "REGISTRY_STORAGE_AZURE_AAD_ENDPOINT"
	// GCP registry env vars
	RegistryStorageGCSBucket        = "REGISTRY_STORAGE_GCS_BUCKET"
	RegistryStorageGCSKeyfile       = "REGISTRY_STORAGE_GCS_KEYFILE"
	RegistryStorageGCSRootdirectory = "REGISTRY_STORAGE_GCS_ROOTDIRECTORY"
)

// provider specific object storage
const (
	S3                    = "s3"
	Azure                 = "azure"
	GCS                   = "gcs"
	AWSProvider           = "aws"
	AzureProvider         = "azure"
	GCPProvider           = "gcp"
	Region                = "region"
	Profile               = "profile"
	S3URL                 = "s3Url"
	S3ForcePathStyle      = "s3ForcePathStyle"
	InsecureSkipTLSVerify = "insecureSkipTLSVerify"
	StorageAccount        = "storageAccount"
	ResourceGroup         = "resourceGroup"
)

// creating skeleton for provider based env var map
var cloudProviderEnvVarMap = map[string][]corev1.EnvVar{
	"aws": {
		{
			Name:  RegistryStorageEnvVarKey,
			Value: S3,
		},
		{
			Name:  RegistryStorageS3AccesskeyEnvVarKey,
			Value: "",
		},
		{
			Name:  RegistryStorageS3BucketEnvVarKey,
			Value: "",
		},
		{
			Name:  RegistryStorageS3RegionEnvVarKey,
			Value: "",
		},
		{
			Name:  RegistryStorageS3SecretkeyEnvVarKey,
			Value: "",
		},
		{
			Name:  RegistryStorageS3RegionendpointEnvVarKey,
			Value: "",
		},
		{
			Name:  RegistryStorageS3SkipverifyEnvVarKey,
			Value: "",
		},
	},
	"azure": {
		{
			Name:  RegistryStorageEnvVarKey,
			Value: Azure,
		},
		{
			Name:  RegistryStorageAzureContainerEnvVarKey,
			Value: "",
		},
		{
			Name:  RegistryStorageAzureAccountnameEnvVarKey,
			Value: "",
		},
		{
			Name:  RegistryStorageAzureAccountkeyEnvVarKey,
			Value: "",
		},
		{
			Name:  RegistryStorageAzureAADEndpointEnvVarKey,
			Value: "",
		},
		{
			Name:  RegistryStorageAzureSPNClientIDEnvVarKey,
			Value: "",
		},
		{
			Name:  RegistryStorageAzureSPNClientSecretEnvVarKey,
			Value: "",
		},
		{
			Name:  RegistryStorageAzureSPNTenantIDEnvVarKey,
			Value: "",
		},
	},
	"gcp": {
		{
			Name:  RegistryStorageEnvVarKey,
			Value: GCS,
		},
		{
			Name:  RegistryStorageGCSBucket,
			Value: "",
		},
		{
			Name:  RegistryStorageGCSKeyfile,
			Value: "",
		},
	},
}

const (
	// Location to store secret if a file is needed
	secretTmpPrefix = "/tmp/registry-"
)

type azureCredentials struct {
	subscriptionID     string
	tenantID           string
	clientID           string
	clientSecret       string
	resourceGroup      string
	strorageAccountKey string
}

func getRegistryEnvVars(bsl *velerov1.BackupStorageLocation) ([]corev1.EnvVar, error) {
	envVar := []corev1.EnvVar{}
	provider := bsl.Spec.Provider
	var err error
	switch provider {
	case AWSProvider:
		envVar, err = getAWSRegistryEnvVars(bsl, cloudProviderEnvVarMap[AWSProvider])

	case AzureProvider:
		envVar, err = getAzureRegistryEnvVars(bsl, cloudProviderEnvVarMap[AzureProvider])

	case GCPProvider:
		envVar, err = getGCPRegistryEnvVars(bsl, cloudProviderEnvVarMap[GCPProvider])
	default:
		return nil, errors.New("unsupported provider")
	}
	if err != nil {
		return nil, err
	}
	return envVar, nil
}

func getAWSRegistryEnvVars(bsl *velerov1.BackupStorageLocation, awsEnvVars []corev1.EnvVar) ([]corev1.EnvVar, error) {
	accessKey, secretKey, err := getAWSCreds(bsl)
	if err != nil {
		return nil, err
	}
	// create secret data and fill up the values and return from here
	for i := range awsEnvVars {
		if awsEnvVars[i].Name == RegistryStorageS3AccesskeyEnvVarKey {
			awsEnvVars[i].Value = accessKey
		}

		if awsEnvVars[i].Name == RegistryStorageS3BucketEnvVarKey {
			awsEnvVars[i].Value = bsl.Spec.StorageType.ObjectStorage.Bucket
		}

		if awsEnvVars[i].Name == RegistryStorageS3RegionEnvVarKey {
			bslSpecRegion, regionInConfig := bsl.Spec.Config[Region]
			if regionInConfig {
				awsEnvVars[i].Value = bslSpecRegion
			} else {
				return nil, errors.New("region not found in backupstoragelocation spec")
			}
		}

		if awsEnvVars[i].Name == RegistryStorageS3SecretkeyEnvVarKey {
			awsEnvVars[i].Value = secretKey
		}

		if awsEnvVars[i].Name == RegistryStorageS3RegionendpointEnvVarKey {
			awsEnvVars[i].Value = bsl.Spec.Config[S3URL]
		}

		if awsEnvVars[i].Name == RegistryStorageS3SkipverifyEnvVarKey {
			awsEnvVars[i].Value = bsl.Spec.Config[InsecureSkipTLSVerify]
		}

	}
	return awsEnvVars, nil
}

func getAzureRegistryEnvVars(bsl *velerov1.BackupStorageLocation, azureEnvVars []corev1.EnvVar) ([]corev1.EnvVar, error) {
	azcreds, err := populateAzureRegistrySecret(bsl)
	if err != nil || azcreds == nil {
		return nil, err
	}
	for i := range azureEnvVars {
		if azureEnvVars[i].Name == RegistryStorageAzureContainerEnvVarKey {
			azureEnvVars[i].Value = bsl.Spec.StorageType.ObjectStorage.Bucket
		}

		if azureEnvVars[i].Name == RegistryStorageAzureAccountnameEnvVarKey {
			azureEnvVars[i].Value = bsl.Spec.Config[StorageAccount]
		}

		if azureEnvVars[i].Name == RegistryStorageAzureAccountkeyEnvVarKey {
			azureEnvVars[i].Value = azcreds.strorageAccountKey
		}
		if azureEnvVars[i].Name == RegistryStorageAzureSPNClientIDEnvVarKey {
			azureEnvVars[i].Value = azcreds.clientID
		}

		if azureEnvVars[i].Name == RegistryStorageAzureSPNClientSecretEnvVarKey {
			azureEnvVars[i].Value = azcreds.clientSecret
		}
		if azureEnvVars[i].Name == RegistryStorageAzureSPNTenantIDEnvVarKey {
			azureEnvVars[i].Value = azcreds.tenantID
		}
	}
	return azureEnvVars, nil
}

func getGCPRegistryEnvVars(bsl *velerov1.BackupStorageLocation, gcpEnvVars []corev1.EnvVar) ([]corev1.EnvVar, error) {
	for i := range gcpEnvVars {
		if gcpEnvVars[i].Name == RegistryStorageGCSBucket {
			gcpEnvVars[i].Value = bsl.Spec.StorageType.ObjectStorage.Bucket
		}

		if gcpEnvVars[i].Name == RegistryStorageGCSKeyfile {
			// check for secret name
			secretName, secretKey := getSecretNameAndKey(&bsl.Spec, oadpv1alpha1.DefaultPluginGCP)
			// get secret value and save it to /tmp/registry-<secretName>

			secretEnvVarSource := &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				Key:                  secretKey,
			}
			secretData, err := getSecretKeyRefData(secretEnvVarSource, bsl.Namespace)
			if err != nil {
				return nil, err
			}
			// write secret data to /tmp/registry-<secretName>
			secretPath := secretTmpPrefix + secretName
			err = saveDataToFile(secretData, secretPath)
			if err != nil {
				return nil, err
			}
			gcpEnvVars[i].Value = secretPath
		}
	}
	return gcpEnvVars, nil
}

func getSecretNameAndKey(bslSpec *velerov1.BackupStorageLocationSpec, plugin oadpv1alpha1.DefaultPlugin) (string, string) {
	// Assume default values unless user has overriden them
	secretName := credentials.PluginSpecificFields[plugin].SecretName
	secretKey := credentials.PluginSpecificFields[plugin].PluginSecretKey
	if _, ok := bslSpec.Config["credentialsFile"]; ok {
		if secretName, secretKey, err :=
			credentials.GetSecretNameKeyFromCredentialsFileConfigString(bslSpec.Config["credentialsFile"]); err == nil {
			return secretName, secretKey
		}
	}
	// check if user specified the Credential Name and Key
	credential := bslSpec.Credential
	if credential != nil {
		if len(credential.Name) > 0 {
			secretName = credential.Name
		}
		if len(credential.Key) > 0 {
			secretKey = credential.Key
		}
	}

	return secretName, secretKey
}

// get access key and secret key from BSL spec.
func getAWSCreds(bsl *velerov1.BackupStorageLocation) (string, string, error) {
	// Check for secret name
	secretName, secretKey := getSecretNameAndKey(&bsl.Spec, oadpv1alpha1.DefaultPluginAWS)

	// fetch secret and error
	secret, err := getProviderSecret(secretName)
	if err != nil {
		return "", "", errors.Wrapf(err, "Error fetching provider secret %s for backupstoragelocation %s/%s", secretName, bsl.Namespace, bsl.Name)
	}
	awsProfile := "default"
	if value, exists := bsl.Spec.Config[Profile]; exists {
		awsProfile = value
	}
	// parse the secret and get aws access_key and aws secret_key
	AWSAccessKey, AWSSecretKey, err := parseAWSSecret(secret, secretKey, awsProfile)
	if err != nil {
		return "", "", errors.Wrap(err, fmt.Sprintf("Error parsing provider secret %s for backupstoragelocation %s/%s", secretName, bsl.Namespace, bsl.Name))
	}
	return AWSAccessKey, AWSSecretKey, nil
}

func populateAzureRegistrySecret(bsl *velerov1.BackupStorageLocation) (*azureCredentials, error) {
	// Check for secret name
	secretName, secretKey := getSecretNameAndKey(&bsl.Spec, oadpv1alpha1.DefaultPluginMicrosoftAzure)

	// fetch secret and error
	secret, err := getProviderSecret(secretName)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Error fetching provider secret %s for backupstoragelocation %s/%s", secretName, bsl.Namespace, bsl.Name))
	}

	// parse the secret and get azure storage account key
	azcreds, err := parseAzureSecret(secret, secretKey)
	if err != nil {
		return nil, errors.Wrapf(err, "Error parsing provider secret %s for backupstoragelocation %s/%s", secretName, bsl.Namespace, bsl.Name)
	}

	if len(bsl.Spec.Config["storageAccountKeyEnvVar"]) != 0 {
		if azcreds.strorageAccountKey == "" {
			return nil, errors.New("no strorageAccountKey value present in credentials file")
		}
	} else {
		if len(azcreds.subscriptionID) == 0 &&
			len(azcreds.tenantID) == 0 &&
			len(azcreds.clientID) == 0 &&
			len(azcreds.clientSecret) == 0 &&
			len(azcreds.resourceGroup) == 0 {
			return nil, errors.New("error finding service principal parameters for the supplied Azure credential")
		}
	}

	return &azcreds, nil
}

func getProviderSecret(secretName string) (corev1.Secret, error) {
	podNamespace, err := podNamespace()
	if err != nil {
		return corev1.Secret{}, err
	}
	key := types.NamespacedName{
		Name:      secretName,
		Namespace: podNamespace,
	}
	cv1c, err := clients.CoreClient()
	if err != nil {
		return corev1.Secret{}, err
	}
	secret, err :=  cv1c.Secrets(key.Namespace).Get(context.TODO(), key.Name, v1.GetOptions{})
	if err != nil {
		return corev1.Secret{}, err
	}
	// replace carriage return with new line
	secret.Data = replaceCarriageReturn(secret.Data)
	cv1c.Secrets(key.Namespace).Update(context.TODO(), secret, v1.UpdateOptions{})
	return *secret, nil
}
// looks up namespace pod is in from /var/run/secrets/kubernetes.io/serviceaccount/namespace
func podNamespace() (string, error) {
	ns, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		err = errors.Wrap(err, "unable to read namespace from /var/run/secrets/kubernetes.io/serviceaccount/namespace")
	}
	
	return string(ns), err
}

func replaceCarriageReturn(data map[string][]byte) map[string][]byte {
	for k, v := range data {
		// report if carriage return is found
		if strings.Contains(string(v), "\r\n") {
			// log.Info("carriage return replaced")
			data[k] = []byte(strings.ReplaceAll(string(v), "\r\n", "\n"))
		}
	}
	return data
}

func parseAWSSecret(secret corev1.Secret, secretKey string, matchProfile string) (string, string, error) {

	AWSAccessKey, AWSSecretKey, profile := "", "", ""
	splitString := strings.Split(string(secret.Data[secretKey]), "\n")
	keyNameRegex, err := regexp.Compile(`\[.*\]`)
	const (
		accessKeyKey = "aws_access_key_id"
		secretKeyKey = "aws_secret_access_key"
	)
	if err != nil {
		return AWSAccessKey, AWSSecretKey, errors.New("parseAWSSecret faulty regex: keyNameRegex")
	}
	awsAccessKeyRegex, err := regexp.Compile(`\b` + accessKeyKey + `\b`)
	if err != nil {
		return AWSAccessKey, AWSSecretKey, errors.New("parseAWSSecret faulty regex: awsAccessKeyRegex")
	}
	awsSecretKeyRegex, err := regexp.Compile(`\b` + secretKeyKey + `\b`)
	if err != nil {
		return AWSAccessKey, AWSSecretKey, errors.New("parseAWSSecret faulty regex: awsSecretKeyRegex")
	}
	for index, line := range splitString {
		if line == "" {
			continue
		}
		if keyNameRegex.MatchString(line) {
			awsProfileRegex, err := regexp.Compile(`\[|\]`)
			if err != nil {
				return AWSAccessKey, AWSSecretKey, errors.New("parseAWSSecret faulty regex: keyNameRegex")
			}
			cleanedLine := strings.ReplaceAll(line, " ", "")
			parsedProfile := awsProfileRegex.ReplaceAllString(cleanedLine, "")
			if parsedProfile == matchProfile {
				profile = matchProfile
				// check for end of arr
				if index+1 >= len(splitString) {
					break
				}
				for _, profLine := range splitString[index+1:] {
					if profLine == "" {
						continue
					}
					matchedAccessKey := awsAccessKeyRegex.MatchString(profLine)
					matchedSecretKey := awsSecretKeyRegex.MatchString(profLine)

					if err != nil {
						return AWSAccessKey, AWSSecretKey, errors.WithStack(err)
					}
					if matchedAccessKey { // check for access key
						AWSAccessKey, err = getMatchedKeyValue(accessKeyKey, profLine)
						if err != nil {
							return AWSAccessKey, AWSSecretKey, errors.WithStack(err)
						}
						continue
					} else if matchedSecretKey { // check for secret key
						AWSSecretKey, err = getMatchedKeyValue(secretKeyKey, profLine)
						if err != nil {
							return AWSAccessKey, AWSSecretKey, errors.WithStack(err)
						}
						continue
					} else {
						break // aws credentials file is only allowed to have profile followed by aws_access_key_id, aws_secret_access_key
					}
				}
			}
		}
	}
	if profile == "" {
		return AWSAccessKey, AWSSecretKey, errors.New("error finding AWS Profile for the supplied AWS credential")
	}
	if AWSAccessKey == "" {
		return AWSAccessKey, AWSSecretKey, errors.New("error finding access key id for the supplied AWS credential")
	}
	if AWSSecretKey == "" {
		return AWSAccessKey, AWSSecretKey, errors.New("error finding secret access key for the supplied AWS credential")
	}

	return AWSAccessKey, AWSSecretKey, nil
}

func parseAzureSecret(secret corev1.Secret, secretKey string) (azureCredentials, error) {

	azcreds := azureCredentials{}

	splitString := strings.Split(string(secret.Data[secretKey]), "\n")
	keyNameRegex, err := regexp.Compile(`\[.*\]`) //ignore lines such as [default]
	if err != nil {
		return azcreds, errors.New("parseAzureSecret faulty regex: keyNameRegex")
	}
	azureStorageKeyRegex, err := regexp.Compile(`\bAZURE_STORAGE_ACCOUNT_ACCESS_KEY\b`)
	if err != nil {
		return azcreds, errors.New("parseAzureSecret faulty regex: azureStorageKeyRegex")
	}
	azureTenantIdRegex, err := regexp.Compile(`\bAZURE_TENANT_ID\b`)
	if err != nil {
		return azcreds, errors.New("parseAzureSecret faulty regex: azureTenantIdRegex")
	}
	azureClientIdRegex, err := regexp.Compile(`\bAZURE_CLIENT_ID\b`)
	if err != nil {
		return azcreds, errors.New("parseAzureSecret faulty regex: azureClientIdRegex")
	}
	azureClientSecretRegex, err := regexp.Compile(`\bAZURE_CLIENT_SECRET\b`)
	if err != nil {
		return azcreds, errors.New("parseAzureSecret faulty regex: azureClientSecretRegex")
	}
	azureResourceGroupRegex, err := regexp.Compile(`\bAZURE_RESOURCE_GROUP\b`)
	if err != nil {
		return azcreds, errors.New("parseAzureSecret faulty regex: azureResourceGroupRegex")
	}
	azureSubscriptionIdRegex, err := regexp.Compile(`\bAZURE_SUBSCRIPTION_ID\b`)
	if err != nil {
		return azcreds, errors.New("parseAzureSecret faulty regex: azureSubscriptionIdRegex")
	}
	for _, line := range splitString {
		if line == "" {
			continue
		}
		if keyNameRegex.MatchString(line) {
			continue
		}
		// check for storage key
		matchedStorageKey := azureStorageKeyRegex.MatchString(line)
		matchedSubscriptionId := azureSubscriptionIdRegex.MatchString(line)
		matchedTenantId := azureTenantIdRegex.MatchString(line)
		matchedCliendId := azureClientIdRegex.MatchString(line)
		matchedClientsecret := azureClientSecretRegex.MatchString(line)
		matchedResourceGroup := azureResourceGroupRegex.MatchString(line)

		switch {
		case matchedStorageKey:
			storageKeyValue, err := getMatchedKeyValue("AZURE_STORAGE_ACCOUNT_ACCESS_KEY", line)
			if err != nil {
				return azcreds, err
			}
			azcreds.strorageAccountKey = storageKeyValue
		case matchedSubscriptionId:
			subscriptionIdValue, err := getMatchedKeyValue("AZURE_SUBSCRIPTION_ID", line)
			if err != nil {
				return azcreds, err
			}
			azcreds.subscriptionID = subscriptionIdValue
		case matchedCliendId:
			clientIdValue, err := getMatchedKeyValue("AZURE_CLIENT_ID", line)
			if err != nil {
				return azcreds, err
			}
			azcreds.clientID = clientIdValue
		case matchedClientsecret:
			clientSecretValue, err := getMatchedKeyValue("AZURE_CLIENT_SECRET", line)
			if err != nil {
				return azcreds, err
			}
			azcreds.clientSecret = clientSecretValue
		case matchedResourceGroup:
			resourceGroupValue, err := getMatchedKeyValue("AZURE_RESOURCE_GROUP", line)
			if err != nil {
				return azcreds, err
			}
			azcreds.resourceGroup = resourceGroupValue
		case matchedTenantId:
			tenantIdValue, err := getMatchedKeyValue("AZURE_TENANT_ID", line)
			if err != nil {
				return azcreds, err
			}
			azcreds.tenantID = tenantIdValue
		}
	}
	return azcreds, nil
}

// Return value to the right of = sign with quotations and spaces removed.
func getMatchedKeyValue(key string, s string) (string, error) {
	for _, removeChar := range []string{"\"", "'", " "} {
		s = strings.ReplaceAll(s, removeChar, "")
	}
	for _, prefix := range []string{key, "="} {
		s = strings.TrimPrefix(s, prefix)
	}
	if len(s) == 0 {
		return s, errors.New(key + " secret parsing error")
	}
	return s, nil
}
