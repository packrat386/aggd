package aggd

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/google/uuid"
)

type diskDAL struct{}

func (d *diskDAL) Write(ctx context.Context, dd []Datapoint) error {
	panic("not implemented")
}

func (d *diskDAL) KeysMatching(ctx context.Context, pattern string) ([]string, error) {
	panic("not implemented")
}

func (d *diskDAL) Read(ctx context.Context, keyPattern string, tr TimeRange) ([]Datapoint, error) {
	panic("not implemented")
}

type index struct {
	f *os.File
	l Locker
}

func (i *index) dataLocationFor(_ context.Context, path Path) (uuid.UUID, error) {
	panic("not implemented")
}

func (i *index) readPage(ctx context.Context, n int64) (indexPage, error) {
	// todo: rwlock?
	i.l.Lock(ctx, "$index")
	defer i.l.Unlock(ctx, "$index")

	page := indexPage{}
	buf := make([]byte, indexPageSize)

	_, err := i.f.ReadAt(buf, n*indexPageSize)
	if err != nil {
		return page, fmt.Errorf("could not read page: %w", err)
	}

	err = binary.Read(bytes.NewBuffer(buf), binary.LittleEndian, &page)
	if err != nil {
		return page, fmt.Errorf("could not deserialize page information: %w", err)
	}

	return page, nil
}

func (i *index) writePage(ctx context.Context, n int64, page indexPage) error {
	i.l.Lock(ctx, "$index")
	defer i.l.Unlock(ctx, "$index")

	buf := new(bytes.Buffer)

	err := binary.Write(buf, binary.LittleEndian, page)
	if err != nil {
		return fmt.Errorf("could not serialize page information: %w", err)
	}

	_, err = i.f.WriteAt(buf.Bytes(), n*indexPageSize)
	if err != nil {
		return fmt.Errorf("could not write page: %w", err)
	}

	i.f.Sync()

	return nil
}

const indexPageSize = 4096

type indexPage struct {
	Next    int64
	Entries [73]indexEntry
}

type indexEntry struct {
	Key          [32]byte
	IndexPage    int64
	DataLocation uuid.UUID
}
