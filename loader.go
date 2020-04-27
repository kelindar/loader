// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package loader

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/kelindar/loader/file"
	"github.com/kelindar/loader/gcs"
	"github.com/kelindar/loader/http"
	"github.com/kelindar/loader/s3"
)

var (
	zeroTime = time.Unix(0, 0)
	timeout  = 30 * time.Second
)

// Loader represents a client that can load something from a remote source.
type Loader struct {
	watchers sync.Map     // The list of watchers
	s3       *s3.Client   // The client for AWS S3
	web      *http.Client // The client for HTTP
	fs       *file.Client // The client for the filesystem
	gcs      *gcs.Client  // The client for Google Cloud Storage
}

// New creates a new loader instance.
func New(options ...func(*Loader)) *Loader {
	s3, err := s3.New("", 5)
	if err != nil {
		panic(err)
	}

	gcs, err := gcs.New()
	if err != nil {
		panic(err)
	}

	loader := &Loader{
		fs:  file.New(),
		web: http.New(),
		s3:  s3,
		gcs: gcs,
	}

	for _, option := range options {
		option(loader)
	}

	return loader
}

// WithS3Client sets loader with S3Client
func WithS3Client(s3Client *s3.Client) func(*Loader) {
	return func(l *Loader) {
		l.s3 = s3Client
	}
}

// WithHTTPClient sets loader with HttpClient
func WithHTTPClient(httpClient *http.Client) func(*Loader) {
	return func(l *Loader) {
		l.web = httpClient
	}
}

// Load attempts to load the resource from the specified URL.
func (l *Loader) Load(ctx context.Context, uri string) ([]byte, error) {
	return l.LoadIf(ctx, uri, zeroTime)
}

// LoadIf attempts to load the resource from the specified URL but only if it's more recent
// than the specified 'updatedSince' time.
func (l *Loader) LoadIf(ctx context.Context, uri string, updatedSince time.Time) ([]byte, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(u.Scheme) {
	case "file":
		return l.fs.DownloadIf(uri, updatedSince)
	case "http", "https":
		return l.web.DownloadIf(uri, updatedSince)
	case "s3":
		return l.s3.DownloadIf(ctx, getBucket(u.Host), getPrefix(u.Path), updatedSince)
	case "gcs", "cs":
		return l.gcs.DownloadIf(ctx, getBucket(u.Host), getPrefix(u.Path), updatedSince)
	}

	return nil, fmt.Errorf("scheme %s is not supported", u.Scheme)
}

// Watch starts watching a specific URI
func (l *Loader) Watch(ctx context.Context, uri string, interval time.Duration) <-chan Update {
	w, loaded := l.watchers.LoadOrStore(uri, newWatcher(l, uri, interval))
	watch := w.(*watcher)

	// If it's a new watch, start it (otherwise we return an existing watch channel)
	if !loaded {
		watch.Start(ctx)
	}
	return watch.updates
}

// Unwatch stops watching a specific URI
func (l *Loader) Unwatch(uri string) bool {
	if w, ok := l.watchers.Load(uri); ok {
		w.(*watcher).Stop()
		return true
	}
	return false
}

func getBucket(host string) string {
	return strings.Split(host, ".")[0]
}

func getPrefix(path string) string {
	return strings.TrimLeft(path, "/")
}
