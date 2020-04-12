// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package s3

import (
	"context"
	"errors"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
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
		conf.WithEndpoint(region).WithS3ForcePathStyle(true)
	case region != "":
		conf.WithRegion(region)
	case os.Getenv("AWS_DEFAULT_REGION") != "":
		conf.WithRegion(os.Getenv("AWS_DEFAULT_REGION"))
	default:
		conf.WithRegion("us-east-1")
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
func (s *Client) DownloadIf(ctx context.Context, bucket, prefix string, updatedSince time.Time) ([]byte, error) {
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

// getLatestKey returns latest uploaded key in given bucket
func (s *Client) getLatestKey(ctx context.Context, bucket, prefix string) (string, time.Time, error) {
	list, err := s.client.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return "", time.Time{}, convertError(err)
	}

	// search for the latest object within the prefix
	var updatedKey string
	var updatedAt time.Time
	for _, object := range list.Contents {
		if aws.Int64Value(object.Size) > 0 && isModified(aws.TimeValue(object.LastModified), updatedAt) {
			updatedKey = aws.StringValue(object.Key)
			updatedAt = aws.TimeValue(object.LastModified)
		}
	}

	if updatedKey == "" {
		return "", time.Time{}, ErrNoSuchKey
	}
	return updatedKey, updatedAt, nil
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
