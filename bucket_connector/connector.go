package bucket_connector

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/google/uuid"
	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var logger *zap.Logger

const (
	DefaultBucketName = "example.com"
	DefaultJsonKey    = "gcp.json"
)

type UploaderReq struct {
	FileName string `json:"file_name"`
	Category string `json:"category"`
	RawData  string `json:"rowData"`
}

type BucketConnector struct {
	params Params
	logger *zap.Logger
	client *storage.Client
	scope  string
}

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *zap.Logger
}

func Module(scope string) fx.Option {

	var m *BucketConnector

	return fx.Module(
		scope,
		fx.Provide(func(p Params) *BucketConnector {

			logger = p.Logger.Named(scope)

			m := &BucketConnector{
				params: p,
				logger: logger,
				scope:  scope,
			}

			m.initDefaultConfigs()

			return m
		}),
		fx.Populate(&m),
		fx.Invoke(func(p Params) *BucketConnector {

			p.Lifecycle.Append(
				fx.Hook{
					OnStart: m.onStart,
					OnStop:  m.onStop,
				},
			)

			return m
		}),
	)
}

func (c *BucketConnector) getConfigPath(key string) string {
	return fmt.Sprintf("%s.%s", c.scope, key)
}

func (c *BucketConnector) initDefaultConfigs() {
	viper.SetDefault(c.getConfigPath("bucket_name"), DefaultBucketName)
	viper.SetDefault(c.getConfigPath("json_key"), DefaultJsonKey)
}

func (c *BucketConnector) onStart(ctx context.Context) error {

	jsonKey := viper.GetString(c.getConfigPath("json_key"))

	logger.Info("Starting BucketConnector",
		zap.String("bucket_name", viper.GetString(c.getConfigPath("bucket_name"))),
		zap.String("json_key", jsonKey),
	)

	client, err := storage.NewClient(ctx, option.WithCredentialsFile(jsonKey))
	if err != nil {
		c.logger.Error("storage.NewClient Error")
		return err
	}

	c.client = client

	return nil
}

func (c *BucketConnector) onStop(ctx context.Context) error {

	c.logger.Info("Stopped BucketConnector")

	return c.client.Close()
}

func (c *BucketConnector) GetBucket() *storage.BucketHandle {
	bucketName := viper.GetString(c.getConfigPath("bucket_name"))
	return c.client.Bucket(bucketName)
}

func (c *BucketConnector) DeleteFileWithPrefix(filePath string) error {

	bucket := c.GetBucket()

	// Delete the objects with the prefix
	it := bucket.Objects(context.Background(), &storage.Query{
		Prefix: filePath,
	})
	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		err = bucket.Object(objAttrs.Name).Delete(context.Background())
		if err != nil && err != storage.ErrObjectNotExist {
			return err
		}
	}

	return nil
}

func (c *BucketConnector) DeleteFile(filePath string) error {

	bucket := c.GetBucket()

	// Delete the object
	err := bucket.Object(filePath).Delete(context.Background())
	if err != nil {

		if err == storage.ErrObjectNotExist {
			return nil
		}

		return err
	}

	return nil
}

func (c *BucketConnector) WriteAsFile(filePath string, content []byte) (string, error) {

	bucket := c.GetBucket()
	w := bucket.Object(filePath).NewWriter(context.Background())
	w.ACL = []storage.ACLRule{
		{
			Entity: storage.AllUsers,
			Role:   storage.RoleReader,
		},
	}

	// Write the content to the bucket
	reader := bytes.NewReader(content)
	if _, err := io.Copy(w, reader); err != nil {
		return "", err
	}
	if err := w.Close(); err != nil {
		return "", err
	}

	// Preparing external URL
	u, err := url.Parse(fmt.Sprintf("%v/%v", w.Attrs().Bucket, w.Attrs().Name))
	if err != nil {
		return "", err
	}

	url := fmt.Sprintf("https://%s", u.EscapedPath())

	return url, nil
}

func (c *BucketConnector) SaveFile(req *UploaderReq) (string, error) {
	// new a bucket client
	ctx := context.Background()

	// decode, err := base64.StdEncoding.DecodeString(req.Data)
	reader := base64.NewDecoder(base64.StdEncoding, strings.NewReader(req.RawData))

	// init uploder
	fileName := uuid.New().String()
	if req.FileName != "" {
		fileName = req.FileName
	}

	filePath := fmt.Sprintf("%s/%s", req.Category, fileName)

	bucket := c.client.Bucket(viper.GetString(c.getConfigPath("bucket_name")))
	w := bucket.Object(filePath).NewWriter(ctx)
	w.ACL = []storage.ACLRule{{Entity: storage.AllUsers, Role: storage.RoleReader}}

	// upload to bucket
	if _, err := io.Copy(w, reader); err != nil {
		c.logger.Error("io.Copy Error")
		return "", err
	}
	if err := w.Close(); err != nil {
		c.logger.Error("io.Close Error")
		return "", err
	}

	u, err := url.Parse(fmt.Sprintf("%v/%v", w.Attrs().Bucket, w.Attrs().Name))
	if err != nil {
		c.logger.Error("url.Parse Error")
		return "", err
	}

	url := fmt.Sprintf("https://%s", u.EscapedPath())

	return url, nil
}

func (c *BucketConnector) GetClient() *storage.Client {
	return c.client
}
