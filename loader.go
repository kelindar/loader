// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package loader

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/kelindar/loader/http"
	"github.com/kelindar/loader/s3"
)

var zeroTime = time.Unix(0, 0)

// Loader represents a client that can load something from a remote source.
type Loader struct {
	s3  *s3.Client   // The client for AWS S3
	web *http.Client // The client for HTTP
}

// New creates a new loader instance.
func New(options ...func(*Loader)) *Loader {
	s3cli, err := s3.New("", 5)
	if err != nil {
		panic(err)
	}

	loader := &Loader{
		web: http.New(),
		s3:  s3cli,
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
	case "http", "https":
		return l.web.DownloadIf(uri, updatedSince)
	case "s3":
		return l.s3.DownloadIf(ctx, getBucket(u.Host), getPrefix(u.Path), updatedSince)
	}

	return nil, fmt.Errorf("scheme %s is not supported", u.Scheme)
}

func getBucket(host string) string {
	return strings.Split(host, ".")[0]
}

func getPrefix(path string) string {
	return strings.TrimLeft(path, "/")
}
