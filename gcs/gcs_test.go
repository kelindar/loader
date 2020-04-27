// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package gcs

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGCS(t *testing.T) {
	gcs := new(fakeGCS)
	gcs.Objects = make(map[string]object)
	ts := httptest.NewServer(http.HandlerFunc(gcs.serve))
	defer ts.Close()

	// Test data
	bucket := "bucket"
	inputVal := []byte("hello world")
	os.Setenv("STORAGE_EMULATOR_HOST", strings.TrimPrefix(ts.URL, "http://"))
	os.Setenv("STORAGE_EMULATOR_ENDPOINT", ts.URL)

	// Create a new GCS layer
	cli, err := New()
	assert.NotNil(t, cli)
	assert.NoError(t, err)

	// Add a few objects
	gcs.PutObject("hi.txt", inputVal)
	gcs.PutObject("hello.txt", inputVal)

	// Test Download
	{
		val, err := cli.Download(context.Background(), bucket, "hi.txt")
		assert.NoError(t, err)
		assert.Equal(t, inputVal, val)
	}

	// Test DownloadNewer
	{
		val, err := cli.DownloadIf(context.Background(), bucket, "h", time.Unix(0, 0))
		assert.NoError(t, err)
		assert.Equal(t, inputVal, val)
	}
}

// fakeGCS represents a fake GCS server
type fakeGCS struct {
	sync.Mutex
	Objects map[string]object
}

type object struct {
	Key        string
	ModifiedAt int64
	Value      []byte
}

// serve called on every HTTP request
func (s *fakeGCS) serve(w http.ResponseWriter, r *http.Request) {
	s.Lock()
	defer s.Unlock()

	println(r.Method, r.URL.String(), string(valueOf(r)))

	switch {
	case r.Method == http.MethodGet && strings.Contains(r.URL.String(), "/o?"):
		s.ListObjects(w, r)
	case r.Method == http.MethodGet:
		s.GetObject(w, r)
	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

// ListObjects emulates GCS list objects
func (s *fakeGCS) ListObjects(w http.ResponseWriter, r *http.Request) {
	var matches []*Object
	prefix := r.URL.Query().Get("prefix")

	for _, o := range s.Objects {
		if strings.HasPrefix(o.Key, prefix) {
			matches = append(matches, &Object{
				Bucket:  "bucket",
				Name:    o.Key,
				Updated: time.Unix(0, o.ModifiedAt).UTC().Format(time.RFC3339Nano),
				Size:    uint64(len(o.Value)),
			})
		}
	}

	var resp Objects
	resp.Items = matches
	b, _ := json.Marshal(resp)
	w.Write(b)
}

// PutObject emulates GCS put object
func (s *fakeGCS) PutObject(key string, value []byte) {
	s.Objects[key] = object{
		Key:        key,
		ModifiedAt: time.Now().UnixNano(),
		Value:      value,
	}
}

// GetObject emulates GCS get object
func (s *fakeGCS) GetObject(w http.ResponseWriter, r *http.Request) {
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

// Objects: A list of objects.
type Objects struct {
	Items         []*Object `json:"items,omitempty"`
	Kind          string    `json:"kind,omitempty"`
	NextPageToken string    `json:"nextPageToken,omitempty"`
	Prefixes      []string  `json:"prefixes,omitempty"`
}

type Object struct {
	Bucket  string `json:"bucket,omitempty"`
	Name    string `json:"name,omitempty"`
	Updated string `json:"updated,omitempty"`
	Size    uint64 `json:"size,omitempty,string"`
}
