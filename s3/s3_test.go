// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package s3

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestS3(t *testing.T) {
	s3 := new(fakeS3)
	s3.Objects = make(map[string]object)
	ts := httptest.NewServer(http.HandlerFunc(s3.serve))
	defer ts.Close()

	// Test data
	inputVal := []byte("hello world")

	// Create a new S3 layer
	cli, err := New(ts.URL, 5)
	assert.NotNil(t, cli)
	assert.NoError(t, err)

	// Add a few objects
	s3.PutObject("hi.txt", inputVal)
	s3.PutObject("hello.txt", inputVal)

	// Test DownloadNewer
	val, err := cli.DownloadIf(context.Background(), "s3://bucket/h", time.Unix(0, 0))
	assert.NoError(t, err)
	assert.Equal(t, inputVal, val)
}

// fakeS3 represents a fake s3 server
type fakeS3 struct {
	sync.Mutex
	Objects map[string]object
}

type object struct {
	Key        string
	ModifiedAt int64
	Value      []byte
}

// serve called on every HTTP request
func (s *fakeS3) serve(w http.ResponseWriter, r *http.Request) {
	s.Lock()
	defer s.Unlock()

	switch {
	case r.Method == http.MethodGet && strings.Contains(r.URL.String(), "list-type=2&prefix"):
		s.ListObjects(w, r)
	case r.Method == http.MethodGet:
		s.GetObject(w, r)
	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

// ListObjects emulates s3 list objects
func (s *fakeS3) ListObjects(w http.ResponseWriter, r *http.Request) {
	var matches []object
	var sb strings.Builder

	prefix := r.URL.Query().Get("prefix")
	for _, o := range s.Objects {
		if strings.HasPrefix(o.Key, prefix) {
			matches = append(matches, o)
			sb.WriteString(
				fmt.Sprintf(`<Contents><Key>%s</Key><LastModified>%v</LastModified><Size>%d</Size><StorageClass>STANDARD</StorageClass></Contents>`,
					o.Key,
					time.Unix(0, o.ModifiedAt).UTC().Format(time.RFC3339Nano),
					len(o.Value),
				))
		}
	}

	w.Write([]byte(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
	<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		<Name>bucket</Name>
		<Prefix/>
		<KeyCount>%d</KeyCount>
		<MaxKeys>%d</MaxKeys>
		<IsTruncated>false</IsTruncated>
		%s
	</ListBucketResult>`,
		len(matches),
		len(matches),
		sb.String(),
	)))
}

// PutObject emulates s3 put object
func (s *fakeS3) PutObject(key string, value []byte) {
	s.Objects[key] = object{
		Key:        key,
		ModifiedAt: time.Now().UnixNano(),
		Value:      value,
	}
}

// GetObject emulates s3 get object
func (s *fakeS3) GetObject(w http.ResponseWriter, r *http.Request) {
	key := keyOf(r)
	if o, ok := s.Objects[key]; ok {
		w.Write(o.Value)
		return
	}

	w.WriteHeader(http.StatusNotFound)
}

func keyOf(r *http.Request) string {
	url := r.URL.String()
	return url[2+strings.Index(url[1:], "/"):]
}

func valueOf(r *http.Request) []byte {
	defer r.Body.Close()
	v, _ := ioutil.ReadAll(r.Body)
	return v
}
