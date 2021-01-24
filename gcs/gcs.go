// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package gcs

import (
	"context"
	"errors"
	"io/ioutil"
	"net/url"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const scope = storage.ScopeReadOnly

// ErrNoSuchKey is returned when the requested file does not exist
var ErrNoSuchKey = errors.New("key does not exist")

// Client represents the client implementation for the Google Cloud Storage downloader.
type Client struct {
	client *storage.Client
}

// New creates a new client for Google Cloud Storage.
func New() (*Client, error) {
	var opts []option.ClientOption
	if creds, err := loadCredentials(); err == nil {
		opts = append(opts, option.WithCredentials(creds))
	} else {
		opts = append(opts, option.WithScopes(scope))
		opts = append(opts, option.WithoutAuthentication())
	}

	if os.Getenv("STORAGE_EMULATOR_ENDPOINT") != "" {
		opts = append(opts, option.WithEndpoint(os.Getenv("STORAGE_EMULATOR_ENDPOINT")))
	}

	c, err := storage.NewClient(context.Background(), opts...)
	if err != nil {
		return nil, err
	}

	return &Client{
		client: c,
	}, nil
}

// DownloadIf downloads a file only if the updatedSince time is older than the resource
// timestamp itself.
func (s *Client) DownloadIf(ctx context.Context, uri string, updatedSince time.Time) ([]byte, error) {
	bucket, prefix, err := parseURI(uri)
	if err != nil {
		return nil, err
	}

	// Get the latest key
	key, updatedAt, err := s.getLatestKey(ctx, bucket, prefix)
	if err != nil {
		return nil, err
	}

	// If the latest key is older than the time, skip
	if !isModified(updatedAt, updatedSince) {
		return nil, nil
	}

	// Download and return the updatedAt time
	return s.Download(ctx, bucket, key)
}

// Download loads a specified object from the bucket
func (s *Client) Download(ctx context.Context, bucket, key string) ([]byte, error) {
	handle := s.client.Bucket(bucket)
	object := handle.Object(key)

	// Create a new reader for the object
	r, err := object.NewReader(ctx)
	if err != nil {
		return nil, err
	}

	// Read the content
	defer r.Close()
	return ioutil.ReadAll(r)
}

// getLatestKey returns latest uploaded key in given bucket
func (s *Client) getLatestKey(ctx context.Context, bucket, prefix string) (string, time.Time, error) {
	handle := s.client.Bucket(bucket)
	cursor := handle.Objects(ctx, &storage.Query{
		Prefix: prefix,
	})

	var updatedKey string
	var updatedAt time.Time
	for {
		o, err := cursor.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			return "", time.Time{}, err
		}

		if o.Size > 0 && isModified(o.Updated, updatedAt) {
			updatedKey = o.Name
			updatedAt = o.Updated
		}
	}

	if updatedKey == "" {
		return "", time.Time{}, ErrNoSuchKey
	}
	return updatedKey, updatedAt, nil
}

func isModified(updatedAt, updatedSince time.Time) bool {
	return updatedAt.UTC().Unix() > updatedSince.UTC().Unix()
}

// LoadCredentials loads the appropriate credentials
func loadCredentials() (*google.Credentials, error) {
	if v := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS_RAW"); v != "" {
		return google.CredentialsFromJSON(context.Background(), []byte(v), scope)
	}

	return google.FindDefaultCredentials(context.Background(), scope)
}

// parseURI returns bucket and prefix
func parseURI(uri string) (string, string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", "", err
	}

	return strings.Split(u.Host, ".")[0], strings.TrimLeft(u.Path, "/"), nil
}
