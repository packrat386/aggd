package aggd

import (
	"context"
	"encoding/binary"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestIndexPageSize(t *testing.T) {
	page := indexPage{}

	assert.Equal(t, indexPageSize, binary.Size(page))
}

func TestIndexPageReadWrite(t *testing.T) {
	p0 := &indexPage{
		Next: 1,
		Entries: [73]indexEntry{
			{
				Key:       padBytes([]byte("foo")),
				IndexPage: 2,
			},
		},
	}

	p1 := &indexPage{
		Next: 0,
		Entries: [73]indexEntry{
			{
				Key:       padBytes([]byte("baz")),
				IndexPage: 3,
			},
		},
	}

	p2 := &indexPage{
		Next: 0,
		Entries: [73]indexEntry{
			{
				Key:          padBytes([]byte("bar")),
				DataLocation: uuid.Must(uuid.NewRandom()),
			},
		},
	}

	p3 := &indexPage{
		Next: 0,
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

func TestDataLocationFor(t *testing.T) {
	loc1 := uuid.Must(uuid.NewRandom())
	loc2 := uuid.Must(uuid.NewRandom())

	p0 := &indexPage{
		Next: 1,
		Entries: [73]indexEntry{
			{
				Key:       padBytes([]byte("foo")),
				IndexPage: 2,
			},
		},
	}

	p1 := &indexPage{
		Next: 0,
		Entries: [73]indexEntry{
			{
				Key:       padBytes([]byte("baz")),
				IndexPage: 3,
			},
		},
	}

	p2 := &indexPage{
		Next: 0,
		Entries: [73]indexEntry{
			{
				Key:          padBytes([]byte("bar")),
				DataLocation: loc1,
			},
		},
	}

	p3 := &indexPage{
		Next: 0,
		Entries: [73]indexEntry{
			{
				Key:          padBytes([]byte("qux")),
				DataLocation: loc2,
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

	got1, err := idx.dataLocationFor(ctx, mustPath("foo.bar"))
	assert.NoError(t, err)
	assert.Equal(t, loc1, got1)

	got2, err := idx.dataLocationFor(ctx, mustPath("baz.qux"))
	assert.NoError(t, err)
	assert.Equal(t, loc2, got2)

	got3, err := idx.dataLocationFor(ctx, mustPath("not.even.close"))
	assert.Zero(t, got3)
	assert.ErrorIs(t, err, errNotFound)

	got4, err := idx.dataLocationFor(ctx, mustPath("foo.baz"))
	assert.Zero(t, got4)
	assert.ErrorIs(t, err, errNotFound)
}

func mustPath(s string) Path {
	p, err := ParsePath(s)
	if err != nil {
		panic(err)
	}

	return p
}
