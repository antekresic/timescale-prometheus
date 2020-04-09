package pgmodel

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/prompb"
)

func getSingleSampleValue(t *testing.T, resp *prompb.ReadResponse) float64 {
	res := resp.GetResults()
	if len(res) != 1 {
		t.Fatal("Expect one result")
	}
	ts := res[0].GetTimeseries()
	if len(ts) != 1 {
		t.Fatal("Expect one timeseries")
	}
	samples := ts[0].GetSamples()
	if len(samples) != 1 {
		t.Fatal("Expect one sample")
	}
	return samples[0].GetValue()
}

func getBooleanSQLResult(t *testing.T, db *pgxpool.Pool, sql string) bool {
	var res *bool = nil
	err := db.QueryRow(context.Background(), sql).Scan(&res)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatalf("NULL found")
	}
	return *res
}

func TestSQLStaleNaN(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	withDB(t, *database, func(db *pgxpool.Pool, t *testing.T) {
		metricName := "StaleMetric"
		metrics := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: metricNameLabelName, Value: metricName},
					{Name: "foo", Value: "bar"},
					{Name: "common", Value: "tag"},
					{Name: "empty", Value: ""},
				},
				Samples: []prompb.Sample{
					{Timestamp: 10, Value: 0.1},
					{Timestamp: 20, Value: math.Float64frombits(value.StaleNaN)},
					{Timestamp: 30, Value: math.NaN()},
					{Timestamp: 40, Value: 0.4},
				},
			},
		}

		ingestor := NewPgxIngestor(db)
		defer ingestor.Close()
		_, err := ingestor.Ingest(metrics)

		if err != nil {
			t.Fatalf("unexpected error while ingesting test dataset: %s", err)
		}

		query := []struct {
			rrq        prompb.ReadRequest
			isNaN      bool
			isStaleNaN bool
		}{
			{
				isStaleNaN: true,
				rrq: prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{
									Type:  prompb.LabelMatcher_EQ,
									Name:  metricNameLabelName,
									Value: metricName,
								},
							},
							StartTimestampMs: 19,
							EndTimestampMs:   21,
						},
					},
				},
			},
			{
				isNaN: true,
				rrq: prompb.ReadRequest{
					Queries: []*prompb.Query{
						{
							Matchers: []*prompb.LabelMatcher{
								{
									Type:  prompb.LabelMatcher_EQ,
									Name:  metricNameLabelName,
									Value: metricName,
								},
							},
							StartTimestampMs: 29,
							EndTimestampMs:   31,
						},
					},
				},
			},
		}

		for _, c := range query {
			r := NewPgxReader(db)
			resp, err := r.Read(&c.rrq)
			startMs := c.rrq.Queries[0].StartTimestampMs
			endMs := c.rrq.Queries[0].EndTimestampMs
			timeClause := fmt.Sprintf("time >= 'epoch'::timestamptz +interval '%d ms' AND "+
				"time <= 'epoch'::timestamptz +interval '%d ms'", startMs, endMs)

			if err != nil {
				t.Fatalf("unexpected error while ingesting test dataset: %s", err)
			}
			answer := getSingleSampleValue(t, resp)
			isStaleNaN := getBooleanSQLResult(t, db,
				fmt.Sprintf(
					`SELECT prom.is_stale_marker(value)
					 FROM prom."StaleMetric"
					 WHERE %s
				`, timeClause))
			isNormalNaN := getBooleanSQLResult(t, db,
				fmt.Sprintf(
					`SELECT prom.is_normal_nan(value)
						 FROM prom."StaleMetric"
						 WHERE %s
					`, timeClause))
			if c.isStaleNaN {
				if !value.IsStaleNaN(answer) {
					t.Fatal("Expected stale NaN, got:", answer)
				}
				if !isStaleNaN {
					t.Fatal("Expected is_stale_marker to return true")
				}
			} else {
				if value.IsStaleNaN(answer) {
					t.Fatal("Got an unexpected stale NaN")
				}
				if isStaleNaN {
					t.Fatal("Expected is_stale_marker to return false")
				}
			}
			if c.isNaN {
				if math.Float64bits(answer) != value.NormalNaN {
					t.Fatal("Expected NaN, got:", answer)
				}
				if !isNormalNaN {
					t.Fatal("Expected is_normal_nan to return true")
				}
			} else {
				if math.Float64bits(answer) == value.NormalNaN {
					t.Fatal("Got an unexpected NaN")
				}
				if isNormalNaN {
					t.Fatal("Expected is_normal_nan to return false")
				}
			}
		}
	})
}
