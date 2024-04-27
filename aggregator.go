package aggd

import (
	"context"
	"fmt"
)

type basicAggregator struct {
	dal DataAccessLayer
}

func (agg *basicAggregator) Write(ctx context.Context, d Datapoint) error {
	// todo: validate path

	// buffer?
	return agg.dal.Write(ctx, []Datapoint{d})
}

func (agg *basicAggregator) Read(ctx context.Context, s SearchParams) ([]SearchResult, error) {
	paths, err := agg.dal.PathsMatching(ctx, s.PathPattern)
	if err != nil {
		return nil, fmt.Errorf("could not find paths matching pattern `%s`: %w", s.PathPattern, err)
	}

	results := []SearchResult{}

	for _, k := range paths {
		for _, b := range s.Buckets {
			data, err := agg.dal.Read(ctx, k, b)
			if err != nil {
				// TODO: error agg
				return nil, fmt.Errorf("could not fetch data for path `%s`: %w", k, err)
			}

			val, err := agg.aggregate(ctx, s.Aggregation, data)
			if err != nil {
				// TODO: error agg
				return nil, fmt.Errorf("failed to aggregate data: %w", err)
			}

			result := SearchResult{
				Path:   k,
				Bucket: b,
				Value:  val,
			}

			results = append(results, result)
		}
	}

	return results, nil
}

func (agg *basicAggregator) aggregate(_ context.Context, aggregation string, data []Datapoint) (int64, error) {
	switch aggregation {
	case "sum":
		return sum(data), nil
	default:
		return 0, fmt.Errorf("unrecognized aggregation: %s", aggregation)
	}
}

func sum(data []Datapoint) int64 {
	var ret int64

	for _, d := range data {
		ret += d.Value
	}

	return ret
}
