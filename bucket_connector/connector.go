package bucket_connector

import (
	"context"
	"fmt"
	"encoding/base64"
	"io"
	"net/url"
	"strings"

	"github.com/spf13/viper"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"github.com/google/uuid"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

var logger *zap.Logger

const (
	DefaultBucketName = "domain.com"
)

type UploaderReq struct {
    FileName string `json:"file_name"`
    Category string `json:"category"`
    RawData  string `json:"rowData"`
}

type BucketConnector struct {
	params Params
	logger *zap.Logger
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

func (c *BucketConnector) onStart(ctx context.Context) error {

	logger.Info("Starting BucketConnector")

	return nil
}

func (c *BucketConnector) onStop(ctx context.Context) error {

	c.logger.Info("Stopped BucketConnector")

	return nil
}

func (c *BucketConnector) SaveFile(bucketJson string, req *UploaderReq) (string, error) {
	// new a bucket client
	ctx := context.Background()

	client, err := storage.NewClient(ctx, option.WithCredentialsFile(bucketJson))
	if err != nil {
		c.logger.Error("NewClient Error")
		return "", err
	}

	// decode, err := base64.StdEncoding.DecodeString(req.Data)
	reader := base64.NewDecoder(base64.StdEncoding, strings.NewReader(req.RawData))

	// init uploder
	fileName := uuid.New().String()
	if req.FileName != "" {
		fileName = req.FileName;
	}

	filePath := fmt.Sprintf("%s/%s", req.Category, fileName)

	bucket := client.Bucket(viper.GetString(c.getConfigPath("bucket_name")))
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
