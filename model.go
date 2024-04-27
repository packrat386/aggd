package aggd

import (
	"context"
	"time"
)

type Datapoint struct {
	Path      Path
	Timestamp time.Time
	Value     int64
}

type SearchParams struct {
	PathPattern Path
	Aggregation string
	Buckets     []TimeRange
}

type SearchResult struct {
	Path   Path
	Bucket TimeRange
	Value  int64
}

type TimeRange struct {
	Begin time.Time
	End   time.Time
}

func (r TimeRange) Contains(t time.Time) bool {
	return r.Begin.Before(t) && r.End.After(t)
}

type Aggregator interface {
	Write(ctx context.Context, d Datapoint) error
	Read(ctx context.Context, s SearchParams) ([]SearchResult, error)
}

type DataAccessLayer interface {
	Write(ctx context.Context, dd []Datapoint) error
	PathsMatching(ctx context.Context, pattern Path) ([]Path, error)
	Read(ctx context.Context, pattern Path, tr TimeRange) ([]Datapoint, error)
}
