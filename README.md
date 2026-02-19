# gcp-modules

GCP service modules for [Weedbox](https://github.com/weedbox) applications, built with [Uber Fx](https://github.com/uber-go/fx) dependency injection.

## Modules

### bucket_connector

A GCP Cloud Storage module that provides bucket operations including file upload, download, and deletion with automatic lifecycle management.

## Installation

```bash
go get github.com/weedbox/gcp-modules
```

## Quick Start

```go
package main

import (
    "github.com/weedbox/gcp-modules/bucket_connector"
    "go.uber.org/fx"
    "go.uber.org/zap"
)

func main() {
    app := fx.New(
        fx.Provide(zap.NewDevelopment),
        bucket_connector.Module("gcp_storage"),
        fx.Invoke(func(bc *bucket_connector.BucketConnector) {
            // Use the connector
        }),
    )
    app.Run()
}
```

## Configuration

The module uses [Viper](https://github.com/spf13/viper) for configuration. All config keys are namespaced under the scope name passed to `Module()`.

| Key | Description | Default |
|-----|-------------|---------|
| `{scope}.bucket_name` | GCP Cloud Storage bucket name | `example.com` |
| `{scope}.json_key` | Path to GCP service account JSON credentials file | `gcp.json` |

### Example

```go
// Register module with scope "gcp_storage"
bucket_connector.Module("gcp_storage")

// Configure via Viper
viper.Set("gcp_storage.bucket_name", "my-bucket.appspot.com")
viper.Set("gcp_storage.json_key", "/path/to/service-account.json")
```

Or via environment variables (with Viper's automatic env binding):

```bash
export GCP_STORAGE_BUCKET_NAME=my-bucket.appspot.com
export GCP_STORAGE_JSON_KEY=/path/to/service-account.json
```

## API Reference

### Module

```go
func Module(scope string) fx.Option
```

Creates an Fx module that provides a `*BucketConnector`. The `scope` parameter namespaces all configuration keys and logger output.

### BucketConnector Methods

#### GetBucket

```go
func (c *BucketConnector) GetBucket() *storage.BucketHandle
```

Returns a handle to the configured GCP bucket for direct bucket operations.

#### GetClient

```go
func (c *BucketConnector) GetClient() *storage.Client
```

Returns the underlying GCP storage client for advanced operations.

#### WriteAsFile

```go
func (c *BucketConnector) WriteAsFile(filePath string, content []byte) (string, error)
```

Writes raw binary data to the bucket with public read access. Returns the public HTTPS URL of the uploaded file.

```go
url, err := bc.WriteAsFile("images/photo.png", imageBytes)
// url: "https://my-bucket.appspot.com/images/photo.png"
```

#### SaveFile

```go
func (c *BucketConnector) SaveFile(req *UploaderReq) (string, error)
```

Saves a base64-encoded file to the bucket. If `FileName` is empty, a UUID is generated automatically. Returns the public HTTPS URL.

```go
url, err := bc.SaveFile(&bucket_connector.UploaderReq{
    Category: "avatars",
    FileName: "profile.jpg",       // optional, auto-generates UUID if empty
    RawData:  base64EncodedString, // base64-encoded file content
})
```

#### DeleteFile

```go
func (c *BucketConnector) DeleteFile(filePath string) error
```

Deletes a single object from the bucket. Returns `nil` if the object does not exist (idempotent).

```go
err := bc.DeleteFile("images/photo.png")
```

#### DeleteFileWithPrefix

```go
func (c *BucketConnector) DeleteFileWithPrefix(filePath string) error
```

Deletes all objects matching the given prefix. Useful for removing all files under a directory.

```go
err := bc.DeleteFileWithPrefix("avatars/user-123/")
```

### Types

#### UploaderReq

```go
type UploaderReq struct {
    FileName string `json:"file_name"` // Target filename (optional, auto-generates UUID if empty)
    Category string `json:"category"`  // Category/directory path in the bucket
    RawData  string `json:"rowData"`   // Base64-encoded file content
}
```

## License

See [LICENSE](LICENSE) for details.
