// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package loader

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
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

func TestUnwatchByCancel(t *testing.T) {
	testCancelByFunc(t, func(wg *sync.WaitGroup, loader *Loader, url string) context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(10*time.Millisecond, func() {
			cancel()
			wg.Done()
		})

		return ctx
	})
}

func TestUnwatchByUrl(t *testing.T) {
	testCancelByFunc(t, func(wg *sync.WaitGroup, loader *Loader, url string) context.Context {
		time.AfterFunc(10*time.Millisecond, func() {
			loader.Unwatch(url)
			wg.Done()
		})

		return context.Background()
	})
}

func testCancelByFunc(t *testing.T, fn func(wg *sync.WaitGroup, loader *Loader, url string) context.Context) {
	loader, url := makeTestLoader()

	var count int64
	var wg sync.WaitGroup
	wg.Add(10)

	// Create a bunch of watchers and unwatch them
	for i := 0; i < 10; i++ {
		handle := fmt.Sprintf("%s-%d", url, i)
		ctx := fn(&wg, loader, handle)

		go func() {
			updates := loader.Watch(ctx, handle, 1*time.Millisecond)
			for range updates {
				atomic.AddInt64(&count, 1)
			}
		}()
	}

	// Wait until everything is finished
	wg.Wait()
	time.Sleep(100 * time.Millisecond)
	assert.GreaterOrEqual(t, int(atomic.LoadInt64(&count)), 10)
	assert.Equal(t, 0, countWatchers(loader))
}

func TestWatchMany(t *testing.T) {
	loader, url := makeTestLoader()
	time.AfterFunc(100*time.Millisecond, func() {
		loader.Unwatch(url)
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()

	var count int
	for i := 0; i < 10; i++ {
		loader.Watch(ctx, url, 1*time.Millisecond)
		cancel()
	}

	updates := loader.Watch(ctx, url, 1*time.Millisecond)
	for range updates {
		count++
	}

	assert.Equal(t, 1, count)
}

func TestWatchTwice(t *testing.T) {
	l, url := makeTestLoader()
	w := newWatcher(l, url, time.Second, func() {})
	assert.NotPanics(t, func() {
		w.Start(context.Background())
		w.Start(context.Background())
	})
}

func TestCheckClosed(t *testing.T) {
	l, url := makeTestLoader()
	w := newWatcher(l, url, time.Second, func() {})
	assert.NotPanics(t, func() {
		w.check(context.Background())
		w.changeState(isCreated, isRunning)
		w.check(context.Background())
	})
}

func TestLoopClosed(t *testing.T) {
	l, url := makeTestLoader()
	w := newWatcher(l, url, time.Millisecond, func() {})
	time.AfterFunc(10*time.Millisecond, func() {
		w.Close()
	})

	// Should not time out
	w.changeState(isCreated, isRunning)
	w.checkLoop(context.Background())
}

func TestRangeWatchers(t *testing.T) {
	loader, url := makeTestLoader()
	loader.Watch(context.Background(), url, 1*time.Millisecond)
	loader.Watch(context.Background(), url, 1*time.Millisecond)

	// Should be one (overwrite)
	assert.Equal(t, 1, countWatchers(loader))
}

func countWatchers(l *Loader) (count int) {
	l.RangeWatchers(func(uri string) bool {
		count++
		return true
	})
	return
}

func makeTestLoader() (*Loader, string) {
	f, _ := filepath.Abs("loader.go")
	return New(), "file:///" + f
}
