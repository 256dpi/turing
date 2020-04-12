package turing

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
)

func TestBackupRestore(t *testing.T) {
	db1, _, err := openDatabase(Config{}, nil, newManager())
	assert.NoError(t, err)

	err = db1.pebble.Set([]byte("foo1"), []byte("bar1"), pebble.NoSync)
	assert.NoError(t, err)

	err = db1.pebble.Set([]byte("foo2"), []byte("bar2"), pebble.NoSync)
	assert.NoError(t, err)

	err = db1.pebble.Set([]byte("foo3"), []byte("bar3"), pebble.NoSync)
	assert.NoError(t, err)

	snapshot, err := db1.snapshot()
	assert.NoError(t, err)

	var buf bytes.Buffer
	err = db1.backup(snapshot, &buf, nil)
	assert.NoError(t, err)

	db2, _, err := openDatabase(Config{}, nil, newManager())
	assert.NoError(t, err)

	err = db2.restore(&buf)
	assert.NoError(t, err)

	iter := db2.pebble.NewIter(nil)

	assert.True(t, iter.First())
	assert.Equal(t, []byte("foo1"), iter.Key())
	assert.Equal(t, []byte("bar1"), iter.Value())

	assert.True(t, iter.Next())
	assert.Equal(t, []byte("foo2"), iter.Key())
	assert.Equal(t, []byte("bar2"), iter.Value())

	assert.True(t, iter.Next())
	assert.Equal(t, []byte("foo3"), iter.Key())
	assert.Equal(t, []byte("bar3"), iter.Value())

	assert.False(t, iter.Next())
	assert.NoError(t, iter.Close())

	assert.NoError(t, db1.close())
	assert.NoError(t, db2.close())
}
