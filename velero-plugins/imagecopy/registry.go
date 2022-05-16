package imagecopy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"

	logstash "github.com/bshuster-repo/logrus-logstash-hook"
	distribution "github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/configuration"
	dcontext "github.com/distribution/distribution/v3/context"
	"github.com/distribution/distribution/v3/manifest/schema1"
	"github.com/distribution/distribution/v3/reference"
	"github.com/distribution/distribution/v3/registry/storage"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"
	"github.com/docker/libtrust"
	"github.com/sirupsen/logrus"
)

var reg distribution.Namespace

// This file contain functions to interact with registry storage without making calls to the a live and serving registry.
// Goal: Upload/Download/Delete images to distribution/distribution compatible registry storage directly without an external server.

// This should read config from ENV. It should be able to adopt current registry ENV that we supports currently for s3, azure, gcs.
func GetConfiguration(args []string) (config *configuration.Configuration, err error) {
	// https://github.com/distribution/distribution/blob/32ccbf193d5016bd0908d2eb636333d3cca22534/configuration/configuration.go#L662-L689
	// Parse parses an input configuration yaml document into a Configuration struct
	// This should generally be capable of handling old configuration format versions
	//
	// Environment variables may be used to override configuration parameters other than version,
	// following the scheme below:
	// Configuration.Abc may be replaced by the value of REGISTRY_ABC,
	// Configuration.Abc.Xyz may be replaced by the value of REGISTRY_ABC_XYZ, and so forth
	// func Parse(rd io.Reader) (*Configuration, error) {
	// 	in, err := ioutil.ReadAll(rd)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	p := configuration.NewParser("registry", []configuration.VersionedParseInfo{
	{
		Version: configuration.MajorMinorVersion(0, 1),
		ParseAs: reflect.TypeOf(configuration.Configuration{}),
		ConversionFunc: func(c interface{}) (interface{}, error) {
			if v0_1, ok := c.(*configuration.Configuration); ok {
				if v0_1.Log.Level == configuration.Loglevel("") {
					if v0_1.Loglevel != configuration.Loglevel("") {
						v0_1.Log.Level = v0_1.Loglevel
					} else {
						v0_1.Log.Level = configuration.Loglevel("info")
					}
				}
				if v0_1.Loglevel != configuration.Loglevel("") {
					v0_1.Loglevel = configuration.Loglevel("")
				}
				if v0_1.Storage.Type() == "" {
					return nil, errors.New("no storage configuration provided")
				}
				return (*configuration.Configuration)(v0_1), nil
			}
			return nil, fmt.Errorf("expected *v0_1Configuration, received %#v", c)
			},
		},
	})
	config = new(configuration.Configuration)
	err = p.Parse([]byte{}, config)
	return config, err
}

func GetRegistry() (distribution.Namespace, error) {
	var err error
	if reg == nil {
		config, err := GetConfiguration([]string{})
		// https://github.com/distribution/distribution/blob/1d33874951b749df7e070b1c702ea418bbc57ed1/registry/root.go#L55-L78
		driver, err := factory.Create(config.Storage.Type(), config.Storage.Parameters())
		if err != nil {
			return nil, err
		}
		ctx := dcontext.Background()
		ctx, err = configureLogging(ctx, config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to configure logging with config: %s", err)
			os.Exit(1)
		}

		k, err := libtrust.GenerateECP256PrivateKey()
		if err != nil {
			fmt.Fprint(os.Stderr, err)
			os.Exit(1)
		}

		registry, err := storage.NewRegistry(ctx, driver, storage.Schema1SigningKey(k))
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to construct registry: %v", err)
			os.Exit(1)
		}
		reg = registry
	}

	return reg, err
}

func GetRepository(ctx context.Context, name string) (repo distribution.Repository, err error) {
	reg, err := GetRegistry()
	if err != nil {
		return nil, err
	}
	refName, err := reference.ParseNamed(name)
	return reg.Repository(dcontext.Background(), refName)
}

func UploadToRepository(ctx context.Context, repo distribution.Repository, ref reference.Named, f *os.File) {
	blobWriter, err := repo.Blobs(dcontext.Background()).Create()
	if err != nil {
		logrus.Errorf("unable to create blob writer: %v", err)
		return
	}
	defer blobWriter.Close()
	manifestService, err := repo.Manifests(dcontext.Background())
	if err != nil {
		logrus.Errorf("unable to create manifest writer: %v", err)
		return
	}
	manifestService.Put(context.Background(), &schema1.Manifest{} )

}

// https://github.com/distribution/distribution/blob/4363fb1ef4676df2b9d99e3630e1b568141597c4/registry/registry.go#L342-L390
// configureLogging prepares the context with a logger using the
// configuration.
func configureLogging(ctx context.Context, config *configuration.Configuration) (context.Context, error) {
	logrus.SetLevel(logLevel(config.Log.Level))

	formatter := config.Log.Formatter
	if formatter == "" {
		formatter = "text" // default formatter
	}

	switch formatter {
	case "json":
		logrus.SetFormatter(&logrus.JSONFormatter{
			TimestampFormat:   time.RFC3339Nano,
			DisableHTMLEscape: true,
		})
	case "text":
		logrus.SetFormatter(&logrus.TextFormatter{
			TimestampFormat: time.RFC3339Nano,
		})
	case "logstash":
		logrus.SetFormatter(&logstash.LogstashFormatter{
			Formatter: &logrus.JSONFormatter{TimestampFormat: time.RFC3339Nano},
		})
	default:
		// just let the library use default on empty string.
		if config.Log.Formatter != "" {
			return ctx, fmt.Errorf("unsupported logging formatter: %q", config.Log.Formatter)
		}
	}

	if config.Log.Formatter != "" {
		logrus.Debugf("using %q logging formatter", config.Log.Formatter)
	}

	if len(config.Log.Fields) > 0 {
		// build up the static fields, if present.
		var fields []interface{}
		for k := range config.Log.Fields {
			fields = append(fields, k)
		}

		ctx = dcontext.WithValues(ctx, config.Log.Fields)
		ctx = dcontext.WithLogger(ctx, dcontext.GetLogger(ctx, fields...))
	}

	dcontext.SetDefaultLogger(dcontext.GetLogger(ctx))
	return ctx, nil
}

// https://github.com/distribution/distribution/blob/4363fb1ef4676df2b9d99e3630e1b568141597c4/registry/registry.go#L392-L400
func logLevel(level configuration.Loglevel) logrus.Level {
	l, err := logrus.ParseLevel(string(level))
	if err != nil {
		l = logrus.InfoLevel
		logrus.Warnf("error parsing level %q: %v, using %q	", level, err, l)
	}

	return l
}