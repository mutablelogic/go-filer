package indexer

import (
	"encoding/json"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
	assert "github.com/stretchr/testify/assert"
)

func Test_mergeCount(t *testing.T) {
	meta := func(key, value string) schema.Meta {
		return schema.Meta{Namespace: schema.FolderNamespace, Key: key, Value: json.RawMessage(value)}
	}
	title := schema.Meta{Namespace: "dc", Key: "title", Value: json.RawMessage(`"x"`)}

	// A listing without counts has the index's, of this version
	indexed := schema.FolderMeta(3, 1)
	assert.Equal(t, append([]schema.Meta{title}, indexed...), mergeCount([]schema.Meta{title}, append([]schema.Meta{title}, indexed...)))

	// or an earlier version's, which aren't lost before it's indexed again
	legacy := []schema.Meta{meta("children", "true"), meta("children-count", "4")}
	assert.Equal(t, legacy, mergeCount(nil, legacy))

	// A listing with counts keeps its own
	listed := schema.FolderMeta(2, 0)
	assert.Equal(t, listed, mergeCount(listed, append(indexed, legacy...)))

	// and a folder which is indexed has the counts of this version, even when
	// its metadata has an earlier version's
	object := &schema.Object{ObjectMeta: schema.ObjectMeta{Meta: legacy}}
	assert.Equal(t, append(legacy, indexed...), withCount(object, folderCount{subfolders: 1, files: 3}).Meta)
}
