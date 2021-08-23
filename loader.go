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
	"github.com/kelindar/loader/http"
)

var (
	zeroTime = time.Unix(0, 0)
	timeout  = 30 * time.Second
)

// Downloader represents a downloader client (e.g. s3, gcs)
type Downloader interface {
	DownloadIf(ctx context.Context, uri string, updatedSince time.Time) ([]byte, error)
}

// Loader represents a client that can load something from a remote source.
type Loader struct {
	watchers sync.Map              // The list of watchers
	clients  map[string]Downloader // The list of dowloaders
}

// New creates a new loader instance.
func New(options ...func(*Loader)) *Loader {
	web := http.New()
	loader := &Loader{
		clients: map[string]Downloader{
			"file":  file.New(),
			"http":  web,
			"https": web,
		},
	}

	for _, option := range options {
		option(loader)
	}

	return loader
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

	// Get the client for the scheme and download
	scheme := strings.ToLower(u.Scheme)
	if client, ok := l.clients[scheme]; ok {
		return client.DownloadIf(ctx, uri, updatedSince)
	}

	return nil, fmt.Errorf("scheme %s is not supported", u.Scheme)
}

// Watch starts watching a specific URI
func (l *Loader) Watch(ctx context.Context, uri string, interval time.Duration) <-chan Update {
	w, loaded := l.watchers.LoadOrStore(uri, newWatcher(l, uri, interval, func() {
		l.Unwatch(uri)
	}))

	// Start the watcher if it's a new one
	watch := w.(*watcher)
	if !loaded {
		watch.Start(ctx)
	}
	return watch.updates
}

// Unwatch stops watching a specific URI
func (l *Loader) Unwatch(uri string) bool {
	if v, loaded := l.watchers.LoadAndDelete(uri); loaded {
		v.(*watcher).Close()
		return true
	}

	return false
}

// RangeWatchers iterates over the currently active watchers by URL. If the
// callback returns false, the iteration is halted.
func (l *Loader) RangeWatchers(fn func(uri string) bool) {
	l.watchers.Range(func(key, value interface{}) bool {
		uri, _ := key.(string)
		return fn(uri)
	})
}

// -------------------------------------------------------------

// WithDownloader registers a downloader for a specific protocol
func WithDownloader(scheme string, dl Downloader) func(*Loader) {
	return func(l *Loader) {
		l.clients[strings.ToLower(scheme)] = dl
	}
}

// WithS3 registers a downloader for the S3 protocol
func WithS3(dl Downloader) func(*Loader) {
	return WithDownloader("s3", dl)
}

// WithGCS registers a downloader for the Google Cloud Storage protocol
func WithGCS(dl Downloader) func(*Loader) {
	return func(l *Loader) {
		l.clients["gs"] = dl
		l.clients["gcs"] = dl
	}
}
