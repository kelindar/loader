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

func TestLoadIf(t *testing.T) {

	f, _ := filepath.Abs("loader.go")
	url := "file:///" + f

	loader := New()
	assert.NotNil(t, loader)
	ctx := context.Background()

	{
		b, err := loader.LoadIf(ctx, url, time.Unix(0, 0))
		assert.NotNil(t, b)
		assert.NoError(t, err)
	}

	{
		b, err := loader.LoadIf(ctx, url, time.Now())
		assert.Nil(t, b)
		assert.NoError(t, err)
	}
}
