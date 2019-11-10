// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package http

import (
	stdhttp "net/http"
	"time"

	"github.com/imroc/req"
)

const timeFormat = stdhttp.TimeFormat

// Client represents the client implementation.
type Client struct {
}

// New creates a new client for HTTP downloads.
func New() *Client {
	return &Client{}
}

// DownloadIf downloads a file only if the updatedSince time is older than the resource
// timestamp itself.
func (c *Client) DownloadIf(uri string, updatedSince time.Time) ([]byte, error) {
	resp, err := req.Head(uri, req.Header{
		"If-Modified-Since": updatedSince.Format(timeFormat),
	})
	if err != nil {
		return nil, err
	}

	// If we got a 304 status code, it's not modified
	if resp.Response().StatusCode == 304 {
		return nil, nil
	}

	// Check for the 'Last-Modified' header
	if lastMod := resp.Response().Header.Get("Last-Modified"); lastMod != "" {
		if updatedAt, err := time.Parse(timeFormat, lastMod); err == nil {
			if !isModified(updatedAt, updatedSince) {
				return nil, nil
			}
		}
	}

	return c.Download(uri)
}

// Download simply downloads a file using an HTTP GET request.
func (c *Client) Download(uri string) ([]byte, error) {
	resp, err := req.Get(uri)
	if err != nil {
		return nil, err
	}

	return resp.ToBytes()
}

func isModified(updatedAt, updatedSince time.Time) bool {
	return updatedAt.UTC().Unix() > updatedSince.UTC().Unix()
}
