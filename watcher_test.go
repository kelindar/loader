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
