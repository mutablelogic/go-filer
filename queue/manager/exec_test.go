// Copyright 2026 David Thorpe
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manager

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	// Packages
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestRegisterTask(t *testing.T) {
	var registry exec
	fn := func(context.Context, json.RawMessage) (any, error) { return nil, nil }

	err := registry.RegisterTask("  Example_Task  ", fn)
	require.NoError(t, err)
	if assert.NotNil(t, registry.t) {
		_, exists := registry.t["example_task"]
		assert.True(t, exists)
	}

	err = registry.RegisterTask("example_task", fn)
	require.Error(t, err)
	assert.True(t, errors.Is(err, httpresponse.ErrConflict))
}

func TestRemoveTask(t *testing.T) {
	var registry exec
	fn := func(context.Context, json.RawMessage) (any, error) { return nil, nil }

	require.NoError(t, registry.RegisterTask("remove_task", fn))
	require.NoError(t, registry.RemoveTask("remove_task"))

	err := registry.RemoveTask("remove_task")
	require.Error(t, err)
	assert.True(t, errors.Is(err, httpresponse.ErrNotFound))
}
