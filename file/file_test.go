// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package file

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFile(t *testing.T) {
	f, _ := filepath.Abs("file.go")
	url := "file:///" + f

	client := New()
	assert.NotNil(t, client)

	{
		b, err := client.DownloadIf(context.Background(), url, time.Unix(0, 0))
		assert.NotNil(t, b)
		assert.NoError(t, err)
	}

	{
		b, err := client.DownloadIf(context.Background(), url, time.Now())
		assert.Nil(t, b)
		assert.NoError(t, err)
	}
}
