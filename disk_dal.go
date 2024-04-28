package aggd

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
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

func (i *index) createDataLocationFor(ctx context.Context, path path, u uuid.UUID) error {
	panic("not implemented")
}

var errNotFound = errors.New("data location not found")

func (i *index) dataLocationFor(ctx context.Context, path Path) (uuid.UUID, error) {
	var (
		currentPage      *indexPage
		currentComponent []byte
		remaining        Path
		ctr              int64
		err              error
	)

	currentPage, err = i.readPage(ctx, 0)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("could not read root page: %w", err)
	}

	currentComponent = path[0]
	remaining = path[1:]

	for {
		// sanity check
		ctr += 1
		if ctr > 1000 {
			return uuid.UUID{}, fmt.Errorf("search too deep")
		}

		entry, found := currentPage.entryFor(currentComponent)
		if found {
			if len(remaining) == 0 { // we're done
				fmt.Println(1)
				if (entry.DataLocation == uuid.UUID{}) {
					return uuid.UUID{}, errNotFound
				}

				return entry.DataLocation, nil
			} else if entry.IndexPage == 0 { // this entry doesn't have anything under it
				fmt.Println(2)
				return uuid.UUID{}, errNotFound
			} else { // search next component in path
				fmt.Println(3)
				currentPage, err = i.readPage(ctx, entry.IndexPage)
				if err != nil {
					return uuid.UUID{}, fmt.Errorf("could not read page [%d]: %w", entry.IndexPage, err)
				}

				currentComponent = remaining[0]
				remaining = remaining[1:]
				continue
			}
		} else {
			if currentPage.Next != 0 { // search next page for this component
				fmt.Println(5)
				currentPage, err = i.readPage(ctx, currentPage.Next)
				if err != nil {
					return uuid.UUID{}, fmt.Errorf("could not read page [%d]: %w", entry.IndexPage, err)
				}

				continue
			} else { // no next page and not on this page
				fmt.Println(6)
				return uuid.UUID{}, errNotFound
			}
		}
	}
}

func (i *index) readPage(ctx context.Context, n int64) (*indexPage, error) {
	// todo: rwlock?
	i.l.Lock(ctx, "$index")
	defer i.l.Unlock(ctx, "$index")

	page := new(indexPage)
	buf := make([]byte, indexPageSize)

	_, err := i.f.ReadAt(buf, n*indexPageSize)
	if err != nil {
		return page, fmt.Errorf("could not read page: %w", err)
	}

	err = binary.Read(bytes.NewBuffer(buf), binary.LittleEndian, page)
	if err != nil {
		return page, fmt.Errorf("could not deserialize page information: %w", err)
	}

	return page, nil
}

func (i *index) writePage(ctx context.Context, n int64, page *indexPage) error {
	i.l.Lock(ctx, "$index")
	defer i.l.Unlock(ctx, "$index")

	buf := new(bytes.Buffer)

	err := binary.Write(buf, binary.LittleEndian, *page)
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

func (i *indexPage) entryFor(key []byte) (indexEntry, bool) {
	padded := padBytes(key)

	for _, e := range i.Entries {
		if padded == e.Key {
			return e, true
		}
	}

	return indexEntry{}, false
}

func (i *indexPage) addIndexPageFor(key []byte, n int64) {
	padded := padBytes(key)

	for idx, e := range i.Entries {
		if padded == e.Key {
			i.Entries[idx].IndexPage = n
		}
	}
}

func (i *indexPage) addDataLocationFor(key []byte, u uuid.UUID) {
	padded := padBytes(key)

	for idx, e := range i.Entries {
		if padded == e.Key {
			i.Entries[idx].DataLocation = u
		}
	}
}

func padBytes(b []byte) [32]byte {
	var ret [32]byte
	copy(ret[:], b)

	return ret
}
