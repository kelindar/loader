// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package s3

import (
	"context"
	"errors"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var (
	// ErrNoSuchBucket is returned when the requested bucket does not exist
	ErrNoSuchBucket = errors.New("bucket does not exist")

	// ErrNoSuchKey is returned when the requested file does not exist
	ErrNoSuchKey = errors.New("key does not exist")
)

// Client represents the client implementation for the S3 downloader.
type Client struct {
	client     *s3.S3
	downloader *s3manager.Downloader
}

// New a new S3 Client.
func New(region string, retries int) (*Client, error) {
	conf := aws.NewConfig().WithMaxRetries(retries)

	// Set the region or endpoint (for testing)
	switch {
	case strings.HasPrefix(region, "http"):
		conf = conf.WithRegion("custom").
			WithEndpoint(region).
			WithS3ForcePathStyle(true).
			WithCredentialsChainVerboseErrors(true).
			WithCredentials(credentials.NewStaticCredentials("XXX", "YYY", ""))
	case region != "":
		conf = conf.WithRegion(region)
	case os.Getenv("AWS_DEFAULT_REGION") != "":
		conf = conf.WithRegion(os.Getenv("AWS_DEFAULT_REGION"))
	default:
		conf = conf.WithRegion("us-east-1")
	}

	// Create the session
	sess, err := session.NewSession(conf)
	if err != nil {
		return nil, err
	}

	return NewFromSession(sess), nil
}

// NewWithConfig creates new S3 Client with passed config
func NewWithConfig(config *aws.Config) (*Client, error) {
	sess, err := session.NewSession(config)
	if err != nil {
		return nil, err
	}
	return NewFromSession(sess), nil
}

// NewFromSession a new S3 Client with the supplied AWS session
func NewFromSession(sess *session.Session) *Client {
	concurrency := runtime.NumCPU() * 4
	return &Client{
		downloader: s3manager.NewDownloader(sess, func(d *s3manager.Downloader) { d.Concurrency = concurrency }),
		client:     s3.New(sess),
	}
}

// DownloadIf downloads a file only if the updatedSince time is older than the resource
// timestamp itself.
func (s *Client) DownloadIf(ctx context.Context, uri string, updatedSince time.Time) ([]byte, error) {
	bucket, key, err := parseURI(uri)
	if err != nil {
		return nil, err
	}

	// Use the head operation to retrieve the last modified date
	head, err := s.client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	switch {
	case err != nil:
		return nil, convertError(err)
	case head.LastModified == nil:
		return nil, nil
	case !isModified(*head.LastModified, updatedSince):
		return nil, nil
	}

	// Download and return the updatedAt time
	return s.Download(ctx, bucket, key)
}

// Download loads a specified object from the bucket
func (s *Client) Download(ctx context.Context, bucket, key string) ([]byte, error) {
	w := new(aws.WriteAtBuffer)
	n, err := s.downloader.DownloadWithContext(ctx, w, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, convertError(err)
	}

	// Trim the buffer and return
	return w.Bytes()[:n], nil
}

// convertError converts the error
func convertError(err error) error {
	if awsErr, ok := err.(awserr.Error); ok {
		switch awsErr.Code() {
		case s3.ErrCodeNoSuchBucket:
			return ErrNoSuchBucket
		case s3.ErrCodeNoSuchKey:
			return ErrNoSuchKey
		}
	}

	return err
}

func isModified(updatedAt, updatedSince time.Time) bool {
	return updatedAt.UTC().Unix() > updatedSince.UTC().Unix()
}

// parseURI returns bucket and prefix
func parseURI(uri string) (string, string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", "", err
	}

	return strings.Split(u.Host, ".")[0], strings.TrimLeft(u.Path, "/"), nil
}
