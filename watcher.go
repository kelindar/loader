// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package loader

import (
	"context"
	"log"
	"runtime/debug"
	"sync/atomic"
	"time"
)

// Various watcher states
const (
	isCreated int32 = iota
	isRunning
	isCanceled
	isDisposed
)

// Update represents a single update event
type Update struct {
	Data []byte // The file contents downloaded
	Err  error  // The error that has occurred during an update
}

// Watcher represents a watcher instance that monitors a single uri
type watcher struct {
	state     int32         // The state machine of the watcher
	updatedAt int64         // The last updated time
	loader    *Loader       // The parent loader to use
	uri       string        // The uri to watch
	updates   chan Update   // The update channel
	interval  time.Duration // Interval between subsequent check calls
	onStop    func()        // User-defined cancellation callback
}

// newWatcher creates a new watcher
func newWatcher(loader *Loader, uri string, interval time.Duration, onStop func()) *watcher {
	return &watcher{
		state:     isCreated,
		updatedAt: 0,
		loader:    loader,
		uri:       uri,
		updates:   make(chan Update, 1),
		interval:  interval,
		onStop:    onStop,
	}
}

// Start starts watching
func (w *watcher) Start(ctx context.Context) {
	if !w.changeState(isCreated, isRunning) {
		return // Prevent from starting twice
	}

	w.check(ctx)
	go w.checkLoop(ctx)
}

// Check performs a single check
func (w *watcher) check(ctx context.Context) {
	switch atomic.LoadInt32(&w.state) {
	case isCanceled: // Manually closed
		w.dispose()
		return
	case isDisposed, isCreated:
		return
	}

	// Timeout only applies for this attempt to fetch,
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	defer handlePanic()

	// Check and load
	now := time.Now()
	b, err := w.loader.LoadIf(ctx, w.uri, w.updatedAtTime())
	if b == nil && err == nil {
		return // No updates, skip
	}

	// Update the time and push the update out
	atomic.StoreInt64(&w.updatedAt, now.UnixNano())
	w.updates <- Update{b, err}
}

// checkLoop calls check on a timer
func (w *watcher) checkLoop(ctx context.Context) {
	for atomic.LoadInt32(&w.state) == isRunning {
		select {
		case <-ctx.Done():
			w.Close()
			w.dispose()
			return
		default:
			w.check(ctx)
			time.Sleep(w.interval)
		}
	}
}

// Close stops the watcher
func (w *watcher) Close() error {
	w.changeState(isRunning, isCanceled)
	w.dispose()
	return nil
}

// dispose closes the channel and marks the watcher as disposed
func (w *watcher) dispose() {
	if w.changeState(isCanceled, isDisposed) {
		close(w.updates)
		w.onStop()
	}
}

// changeState changes the state of the watcher
func (w *watcher) changeState(from, to int32) bool {
	return atomic.CompareAndSwapInt32(&w.state, int32(from), int32(to))
}

// updatedAtTime returns a last updated time
func (w *watcher) updatedAtTime() time.Time {
	return time.Unix(0, atomic.LoadInt64(&w.updatedAt))
}

// handlePanic handles the panic and logs it out.
func handlePanic() {
	if r := recover(); r != nil {
		log.Printf("panic recovered: %s \n %s", r, debug.Stack())
	}
}
