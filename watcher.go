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
	done      chan struct{} // Closed to signal cancellation to the sender
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
		done:      make(chan struct{}),
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
	case isCanceled, isDisposed, isCreated:
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

	// Update the time and push the update out, aborting the send if the watcher
	// is cancelled so we never block on a watcher that's being closed.
	atomic.StoreInt64(&w.updatedAt, now.UnixNano())
	select {
	case w.updates <- Update{b, err}:
	case <-w.done:
	}
}

// checkLoop calls check on a timer. It owns the updates channel and is the only
// goroutine that ever closes it, so sends in check never race with the close.
func (w *watcher) checkLoop(ctx context.Context) {
	defer w.dispose()
	for atomic.LoadInt32(&w.state) == isRunning {
		select {
		case <-ctx.Done():
			w.Close()
			return
		case <-w.done:
			return
		default:
			w.check(ctx)
			time.Sleep(w.interval)
		}
	}
}

// Close stops the watcher. It only signals cancellation; the owning checkLoop
// goroutine observes the signal and disposes the watcher, closing the channel.
func (w *watcher) Close() error {
	if w.changeState(isRunning, isCanceled) {
		close(w.done)
	}
	return nil
}

// dispose closes the channel and marks the watcher as disposed. It must only be
// called from the checkLoop goroutine that owns the updates channel.
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
