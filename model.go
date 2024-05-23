package main

import (
	"fmt"
	"regexp"
	"strconv"
	"time"
)

type ObservedMetric struct {
	Value     int64
	Path      string
	Timestamp time.Time
}

var ObservedMetricFormat = regexp.MustCompile(`^(?P<path>\w+(\.\w+)*):(?P<value>\d+)|(?P<type>[^|]+)(?P<sample>|[^|]+)?$`)

func ParseObservedMetric(s string) (ObservedMetric, error) {
	matches := ObservedMetricFormat.FindStringSubmatch(s)
	if matches == nil {
		return ObservedMetric{}, fmt.Errorf("invalid observed metric")
	}

	path := matches[ObservedMetricFormat.SubexpIndex("path")]
	value, err := strconv.ParseInt(matches[ObservedMetricFormat.SubexpIndex("value")], 10, 64)
	if err != nil {
		return ObservedMetric{}, fmt.Errorf("invalid observed metric")
	}

	return ObservedMetric{
		Value:     value,
		Path:      path,
		Timestamp: time.Now(),
	}, nil
}
