package pgmodel

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/jackc/pgx/v4"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

const (
	microsecFromUnixEpochToY2K = 946684800 * 1000000
)

var (
	errHeaderTooShort        = fmt.Errorf("array header too short")
	errExpectSingleDim       = fmt.Errorf("expected single dimension for array")
	errTooShortOneDim        = fmt.Errorf("array header too short for 1 dimension")
	errInvalidData           = fmt.Errorf("invalid row data")
	errKeysAndValuesMismatch = fmt.Errorf("label keys and values have different lengths")
	errIterInvalidData       = fmt.Errorf("iterator initialized with invalid data")
)

// getArrayInfo returns length of the array, length of the array header and
// any error that occurs while reading it.
func getArrayInfo(src []byte) (int, uint32, error) {
	if len(src) < 12 {
		return 0, 0, errHeaderTooShort
	}

	rp := 0
	// Always expect one dimension.
	if binary.BigEndian.Uint32(src[rp:]) != 1 {
		return 0, 0, errExpectSingleDim
	}

	rp += 4
	// ContainsNull field.
	if binary.BigEndian.Uint32(src[rp:]) == 1 {
		return 0, 12, nil
	}

	// Skipping ElementOID.
	rp += 8

	if len(src) < 20 {
		return 0, 0, errTooShortOneDim
	}

	return int(binary.BigEndian.Uint32(src[rp:])), 20, nil
}

// pgxSeriesSet implements storage.SeriesSet.
type pgxSeriesSet struct {
	rowIdx int
	rows   []pgx.Rows
}

// Next forwards the internal cursor to next storage.Series
func (p *pgxSeriesSet) Next() bool {
	if p.rowIdx >= len(p.rows) {
		return false
	}
	for !p.rows[p.rowIdx].Next() {
		p.rows[p.rowIdx].Close()
		p.rowIdx++
		if p.rowIdx >= len(p.rows) {
			return false
		}
	}
	return true
}

// At returns the current storage.Series. It needs to copy over the
// byte slice since the underlying array changes with each call to pgx.Next().
func (p *pgxSeriesSet) At() storage.Series {
	buf := make([][]byte, 4)
	out := p.rows[p.rowIdx].RawValues()

	for i := range buf {
		buf[i] = make([]byte, len(out[i]))
		copy(buf[i], out[i])
	}
	return &pgxSeries{rows: buf}
}

// Err implements storage.SeriesSet.
func (p *pgxSeriesSet) Err() error {
	return nil
}

// pgxSeries implements storage.Series.
type pgxSeries struct {
	rows [][]byte
	err  error
}

// Labels returns the label names and values for the series.
func (p *pgxSeries) Labels() labels.Labels {
	var ll labels.Labels
	if len(p.rows) < 4 {
		p.err = errInvalidData
		return ll
	}
	keysLength, rp, err := getArrayInfo(p.rows[0])
	if err != nil || keysLength == 0 {
		p.err = err
		return ll
	}
	valsLength, vrp, err := getArrayInfo(p.rows[1])
	if err != nil {
		p.err = err
		return ll
	}

	if valsLength != keysLength || vrp != rp {
		p.err = errKeysAndValuesMismatch
		return ll
	}

	ll = make(labels.Labels, keysLength)

	for i := range ll {
		keyLen := binary.BigEndian.Uint32(p.rows[0][rp:])
		valLen := binary.BigEndian.Uint32(p.rows[1][vrp:])
		rp += 4
		vrp += 4
		ll[i].Name = string(p.rows[0][rp : rp+keyLen])
		ll[i].Value = string(p.rows[1][vrp : vrp+valLen])
		rp += keyLen
		vrp += valLen
	}

	sort.Sort(ll)

	return ll
}

// Iterator returns a chunkenc.Iterator for iterating over series data.
func (p *pgxSeries) Iterator() chunkenc.Iterator {
	if p.err != nil {
		return &pgxSeriesIterator{err: p.err}
	}
	return NewIterator(p.rows)
}

// pgxSeriesIterator implements storage.SeriesIterator.
type pgxSeriesIterator struct {
	cur          int
	totalSamples int
	samples      [][]byte
	err          error
}

func NewIterator(samples [][]byte) *pgxSeriesIterator {
	var (
		rp, vrp uint32
		vLen    int
	)
	p := &pgxSeriesIterator{
		cur: -1,
	}
	p.totalSamples, rp, p.err = getArrayInfo(samples[2])
	if p.err != nil || p.totalSamples == 0 {
		return p
	}
	vLen, vrp, p.err = getArrayInfo(samples[3])
	if p.err != nil {
		return p
	}

	if vLen != p.totalSamples || rp != vrp {
		p.err = errIterInvalidData
		return p
	}

	samples[2] = samples[2][rp:]
	samples[3] = samples[3][vrp:]

	p.samples = samples

	return p
}

// Seek implements storage.SeriesIterator.
func (p *pgxSeriesIterator) Seek(t int64) bool {
	if p.err != nil {
		return false
	}
	found := false
	p.cur = 0

	for !found && p.cur < p.totalSamples {
		if p.getCurrTs() >= t {
			found = true
			break
		}
		p.cur++
	}

	return found
}

func (p *pgxSeriesIterator) getCurrTs() int64 {
	rp := p.cur*12 + 4
	return (microsecFromUnixEpochToY2K + int64(binary.BigEndian.Uint64(p.samples[2][rp:]))) / 1000
}

func (p *pgxSeriesIterator) getCurrVal() float64 {
	rp := p.cur*12 + 4
	return math.Float64frombits(binary.BigEndian.Uint64(p.samples[3][rp:]))
}

// At implements storage.SeriesIterator.
func (p *pgxSeriesIterator) At() (t int64, v float64) {
	if p.err != nil || p.cur >= p.totalSamples {
		return 0, 0
	}
	return p.getCurrTs(), p.getCurrVal()
}

// Next implements storage.SeriesIterator.
func (p *pgxSeriesIterator) Next() bool {
	if p.err != nil {
		return false
	}
	p.cur++
	return p.cur < p.totalSamples
}

// Err implements storage.SeriesIterator.
func (p *pgxSeriesIterator) Err() error {
	return p.err
}
