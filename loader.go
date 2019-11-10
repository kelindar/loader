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
	s3  s3.Client   // The client for AWS S3
	web http.Client // The client for HTTP
}

// New creates a new loader instance.
func New() *Loader {
	return &Loader{}
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
		return l.s3.DownloadIf(ctx, u.Host, u.Path, updatedSince)
	}

	return nil, fmt.Errorf("scheme %s is not supported", u.Scheme)
}
