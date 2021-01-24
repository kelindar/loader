// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package loader

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWatch(t *testing.T) {
	f, _ := filepath.Abs("loader.go")
	url := "file:///" + f

	loader := New()
	assert.NotNil(t, loader)
	ctx := context.Background()

	updates := loader.Watch(ctx, url, 100*time.Millisecond)

	u := <-updates
	assert.NotNil(t, u.Data)
	assert.Nil(t, u.Err)

	assert.True(t, loader.Unwatch(url))
}

func TestWatch_Cancel(t *testing.T) {
	f, _ := filepath.Abs("loader.go")
	url := "file:///" + f
	loader := New()
	assert.NotNil(t, loader)

	var count int
	for i := 0; i < 10; i++ {

		// Create a context and cancel after a while
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(10*time.Millisecond, cancel)

		// This should be unblocked
		updates := loader.Watch(ctx, url, 1*time.Millisecond)
		for range updates {
			count++
		}

	}
	assert.Equal(t, 10, count)
}

func TestWatch_Unwatch(t *testing.T) {
	f, _ := filepath.Abs("loader.go")
	url := "file:///" + f
	loader := New()
	assert.NotNil(t, loader)

	var count int
	for i := 0; i < 10; i++ {
		time.AfterFunc(10*time.Millisecond, func() {
			loader.Unwatch(url)
		})

		// This should be unblocked
		updates := loader.Watch(context.Background(), url, 1*time.Millisecond)
		for range updates {
			count++
		}

	}
	assert.Equal(t, 10, count)
}

func TestWatch_Many(t *testing.T) {
	f, _ := filepath.Abs("loader.go")
	url := "file:///" + f
	loader := New()
	assert.NotNil(t, loader)

	time.AfterFunc(100*time.Millisecond, func() {
		loader.Unwatch(url)
	})

	var count int
	for i := 0; i < 10; i++ {
		loader.Watch(context.Background(), url, 1*time.Millisecond)
	}

	updates := loader.Watch(context.Background(), url, 1*time.Millisecond)
	for range updates {
		count++
	}
	assert.Equal(t, 1, count)
}
