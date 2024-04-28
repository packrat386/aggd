package aggd

import (
	"context"
	"encoding/binary"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func padBytes(b []byte) [32]byte {
	var ret [32]byte
	copy(ret[:], b)

	return ret
}

func TestIndexPageSize(t *testing.T) {
	page := indexPage{}

	assert.Equal(t, indexPageSize, binary.Size(page))
}

func TestIndexPageReadWrite(t *testing.T) {
	p0 := indexPage{
		Next: 1,
		Entries: [73]indexEntry{
			{
				Key:       padBytes([]byte("foo")),
				IndexPage: 2,
			},
		},
	}

	p1 := indexPage{
		Next: -1,
		Entries: [73]indexEntry{
			{
				Key:       padBytes([]byte("baz")),
				IndexPage: 3,
			},
		},
	}

	p2 := indexPage{
		Next: -1,
		Entries: [73]indexEntry{
			{
				Key:          padBytes([]byte("bar")),
				DataLocation: uuid.Must(uuid.NewRandom()),
			},
		},
	}

	p3 := indexPage{
		Next: -1,
		Entries: [73]indexEntry{
			{
				Key:          padBytes([]byte("qux")),
				DataLocation: uuid.Must(uuid.NewRandom()),
			},
		},
	}

	f, err := os.CreateTemp("", "index")
	if err != nil {
		t.Fatalf("could not create tempfile for index: %s", err.Error())
	}
	defer f.Close()

	idx := &index{
		f: f,
		l: NewMemoryLocker(),
	}

	ctx := context.Background()

	assert.NoError(t, idx.writePage(ctx, 3, p3))
	assert.NoError(t, idx.writePage(ctx, 2, p2))
	assert.NoError(t, idx.writePage(ctx, 1, p1))
	assert.NoError(t, idx.writePage(ctx, 0, p0))

	got0, err := idx.readPage(ctx, 0)
	assert.NoError(t, err)
	assert.Equal(t, p0, got0)

	got1, err := idx.readPage(ctx, 1)
	assert.NoError(t, err)
	assert.Equal(t, p1, got1)

	got2, err := idx.readPage(ctx, 2)
	assert.NoError(t, err)
	assert.Equal(t, p2, got2)

	got3, err := idx.readPage(ctx, 3)
	assert.NoError(t, err)
	assert.Equal(t, p3, got3)
}
