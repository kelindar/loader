# URI Loader

This repository contains a small library that helps with downloading things over the URL and can be used to retrieve things from HTTP, S3, File System, GCS and more protocols in the future.

Currrently supported protocols:
 * **Local File System** is supported and added by default, simply use the `file:///` scheme.
 * **HTTP(S)** is supported and enabled by default, simply use the `http://` scheme.
 * **Amazon S3** (or compatible) can be enabled optionally with a recommended `s3://` scheme.
 * **Google Cloud Storage** can be enabled optionally with a recommended `gs://` scheme.

## Usage

In order to use it, you will need to create a new loader with optional S3 or GCS clients as shown in the snippet below.
```go
import (
    "github.com/kelindar/loader"
    "github.com/kelindar/loader/s3"
    "github.com/kelindar/loader/gcs"
)

// ...

loader := New(
    loader.WithDownloader("s3", s3.New("us-east-1", 5)), // Amazon S3 with 5 retries
    loader.WithDownloader("gs", gcs.New()),              // Google Cloud Storage
    )
```

After the initialization, you can simply use it to load an object by its url.

```go
b, err := loader.Load(ctx, "gs://bucket/prefix")
```

It is also possible to conditionally load the object if it was modified since the date/time provided. All of the providers support this functionnality.

```go
b, err := loader.LoadIf(ctx, "s3://bucket/prefix", modifiedSinceTime)
```

Even more useful is the ability to simply watch for the changes, which returns a channel with the updates. For example, in the snippet below the check is performed every 30 seconds and if a file in the prefix was changed, an update is pushed to the channel.

```go
for update := range loader.Watch(ctx, "gs://bucket/prefix", 30*time.Second){
    // Every new update
}
```