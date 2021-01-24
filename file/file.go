// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package file

import (
	"context"
	"io/ioutil"
	"net/url"
	"os"
	"time"
)

// Client represents the client implementation.
type Client struct {
}

// New creates a new client for HTTP downloads.
func New() *Client {
	return &Client{}
}

// DownloadIf downloads a file only if the updatedSince time is older than the resource
// timestamp itself.
func (c *Client) DownloadIf(ctx context.Context, uri string, updatedSince time.Time) ([]byte, error) {
	u, err := parse(uri)
	if err != nil {
		return nil, err
	}

	// Get the file information
	fi, err := os.Stat(u.Path)
	if err != nil {
		return nil, err
	}

	// No updates have happened since the provided date
	if !isModified(fi.ModTime(), updatedSince) {
		return nil, nil
	}

	return c.Download(uri)
}

// Download simply downloads a file using an HTTP GET request.
func (c *Client) Download(uri string) ([]byte, error) {
	u, err := parse(uri)
	if err != nil {
		return nil, err
	}

	// Read the file into a buffer
	return ioutil.ReadFile(u.Path)
}

func parse(uri string) (*url.URL, error) {
	u, err := url.ParseRequestURI(uri)
	if err != nil {
		return nil, err
	}

	// Remove the first slash
	if len(u.Path) > 0 && u.Path[0] == '/' {
		u.Path = u.Path[1:]
	}
	return u, nil
}

func isModified(updatedAt, updatedSince time.Time) bool {
	return updatedAt.UTC().Unix() > updatedSince.UTC().Unix()
}
