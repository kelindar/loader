// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package loader

import (
	"context"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

// Update represents a single update event
type Update struct {
	Data []byte // The file contents downloaded
	Err  error  // The error that has occurred during an update
}

// Watcher represents a watcher instance that monitors a single uri
type watcher struct {
	sync.Mutex
	loader    *Loader       // The parent loader to use
	uri       string        // The uri to watch
	updates   chan Update   // The update channel
	updatedAt time.Time     // The last updated time
	interval  time.Duration // The interval to watch
	timer     *time.Ticker  // The ticker to use
}

// newWatcher creates a new watcher and starts watching a URI
func newWatcher(loader *Loader, uri string, interval time.Duration) *watcher {
	return &watcher{
		loader:    loader,
		uri:       uri,
		updates:   make(chan Update, 1),
		updatedAt: zeroTime,
		interval:  interval,
	}
}

// Check performs a single check
func (w *watcher) check(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	defer handlePanic()

	// Try to load
	now := time.Now()
	b, err := w.loader.LoadIf(ctx, w.uri, w.updatedAt)
	if b == nil && err == nil {
		return // No updates, skip
	}

	// Push an update and update the time
	w.updates <- Update{b, err}
	w.Lock()
	w.updatedAt = now
	w.Unlock()
}

// Start starts watching
func (w *watcher) Start(ctx context.Context) {
	w.Lock()
	defer w.Unlock()

	// Stop previous timer
	if w.timer != nil {
		w.timer.Stop()
	}

	// Start a new timer and a goroutine to monitor
	w.timer = time.NewTicker(w.interval)
	go func() {
		for range w.timer.C {
			w.check(ctx)
		}
	}()
}

// Stop stops the watcher
func (w *watcher) Stop() {
	w.Lock()
	defer w.Unlock()

	// Stop a timer if there is one
	if w.timer != nil {
		w.timer.Stop()
	}
}

// handlePanic handles the panic and logs it out.
func handlePanic() {
	if r := recover(); r != nil {
		log.Printf("panic recovered: %ss \n %s", r, debug.Stack())
	}
}
