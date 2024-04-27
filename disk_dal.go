package aggd

import (
	"context"
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
}

func (i *index) dataLocationFor(_ context.Context, path Path) (uuid.UUID, error) {
	panic("not implemented")
}

const indexPageSize = 4096

type indexPage struct {
	dataLocation uuid.UUID
	next         int64
	padding      [32]byte
	entries      [101]indexEntry
}

type indexEntry struct {
	key  [32]byte
	page int64
}
