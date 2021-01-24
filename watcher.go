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
	cancel    func()        // The cancellation callback
}

// newWatcher creates a new watcher and starts watching a URI
func newWatcher(loader *Loader, uri string, interval time.Duration, cancel func()) *watcher {
	return &watcher{
		loader:    loader,
		uri:       uri,
		updates:   make(chan Update, 1),
		updatedAt: zeroTime,
		interval:  interval,
		cancel:    cancel,
	}
}

// Check performs a single check
func (w *watcher) check(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	defer handlePanic()

	select {
	// If the context is closed, close the watcher instead
	case <-ctx.Done():
		w.Close()

	// Try to load if updated date is different
	default:
		now := time.Now()
		b, err := w.loader.LoadIf(ctx, w.uri, w.updatedAt)
		if b == nil && err == nil {
			return // No updates, skip
		}

		// Update the time and push the update out
		w.Lock()
		w.updatedAt = now
		w.Unlock()
		w.updates <- Update{b, err}
	}
}

// Start starts watching
func (w *watcher) Start(ctx context.Context) {

	// Peform the first check immediately
	w.check(ctx)

	// The rest should be locked
	w.Lock()
	defer w.Unlock()

	// Start a new timer and a goroutine to monitor
	timer := time.NewTicker(w.interval)
	w.timer = timer // Copy
	go func() {
		for range timer.C {
			w.check(ctx)
		}
	}()
}

// Close stops the watcher
func (w *watcher) Close() error {
	w.Lock()
	defer w.Unlock()

	// Avoid double-stopping
	if w.timer != nil {
		w.timer.Stop()   // Stop the timer
		w.timer = nil    // Remove the timer
		close(w.updates) // Close the channel
		w.cancel()       // Call the cancellation callback
	}
	return nil
}

// handlePanic handles the panic and logs it out.
func handlePanic() {
	if r := recover(); r != nil {
		log.Printf("panic recovered: %s \n %s", r, debug.Stack())
	}
}
