package aggd

import (
	"context"
)

type memoryDAL struct {
	data []Datapoint
}

func (m *memoryDAL) Write(_ context.Context, dd []Datapoint) error {
	m.data = append(m.data, dd...)

	return nil
}

func (m *memoryDAL) Read(_ context.Context, path Path, tr TimeRange) ([]Datapoint, error) {
	found := []Datapoint{}

	for _, d := range m.data {
		if d.Path.Equal(path) && tr.Contains(d.Timestamp) {
			found = append(found, d)
		}
	}

	return found, nil
}

func (m *memoryDAL) PathsMatching(_ context.Context, pattern Path) ([]Path, error) {
	found := []Path{}

	for _, d := range m.data {
		if d.Path.MatchesPattern(pattern) {
			found = append(found, d.Path)
		}
	}

	return found, nil
}
