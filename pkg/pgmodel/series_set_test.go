package pgmodel

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
)

type mockPgxRows struct {
	closeCalled  bool
	firstRowRead bool
	idx          int
	results      [][][]byte
}

// Close closes the rows, making the connection ready for use again. It is safe
// to call Close after rows is already closed.
func (m *mockPgxRows) Close() {
	m.closeCalled = true
}

// Err returns any error that occurred while reading.
func (m *mockPgxRows) Err() error {
	panic("not implemented")
}

// CommandTag returns the command tag from this query. It is only available after Rows is closed.
func (m *mockPgxRows) CommandTag() pgconn.CommandTag {
	panic("not implemented")
}

func (m *mockPgxRows) FieldDescriptions() []pgproto3.FieldDescription {
	panic("not implemented")
}

// Next prepares the next row for reading. It returns true if there is another
// row and false if no more rows are available. It automatically closes rows
// when all rows are read.
func (m *mockPgxRows) Next() bool {
	if m.firstRowRead {
		m.idx++
	}
	m.firstRowRead = true

	return m.idx < len(m.results)
}

// Scan reads the values from the current row into dest values positionally.
// dest can include pointers to core types, values implementing the Scanner
// interface, []byte, and nil. []byte will skip the decoding process and directly
// copy the raw bytes received from PostgreSQL. nil will skip the value entirely.
func (m *mockPgxRows) Scan(dest ...interface{}) error {
	panic("not implemented")
}

// Values returns the decoded row values.
func (m *mockPgxRows) Values() ([]interface{}, error) {
	panic("not implemented")
}

// RawValues returns the unparsed bytes of the row values. The returned [][]byte is only valid until the next Next
// call or the Rows is closed. However, the underlying byte data is safe to retain a reference to and mutate.
func (m *mockPgxRows) RawValues() [][]byte {
	return m.results[m.idx]
}

func TestGetArrayInfo(t *testing.T) {

	testCases := []struct {
		name         string
		input        []byte
		length       int
		headerLength uint32
		err          error
	}{
		{
			name:  "empty input",
			input: nil,
			err:   errHeaderTooShort,
		},
		{
			name:  "short input",
			input: []byte("foo"),
			err:   errHeaderTooShort,
		},
		{
			name:  "garbage input",
			input: []byte("roses are red, violets are blue"),
			err:   errExpectSingleDim,
		},
		{
			name:  "more than one dimension",
			input: generateArrayHeader(2, 0, 123, 0, []byte{}),
			err:   errExpectSingleDim,
		},
		{
			name:         "contains null",
			input:        generateArrayHeader(1, 1, 234, 0, []byte{}),
			headerLength: 12,
		},
		{
			name:  "too short for one dimention",
			input: generateArrayHeader(1, 0, 345, 0, []byte{})[:19],
			err:   errTooShortOneDim,
		},
		{
			name:         "happy path",
			input:        generateArrayHeader(1, 0, 456, 1, []byte("additional ignored data")),
			length:       1,
			headerLength: 20,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			length, headerLength, err := getArrayInfo(c.input)

			if length != c.length {
				t.Errorf("unexpected array length: got %d, wanted %d\n", length, c.length)
			}

			if headerLength != c.headerLength {
				t.Errorf("unexpected array header length: got %d, wanted %d\n", headerLength, c.headerLength)
			}

			if err != nil && err != c.err {
				t.Errorf("unexpected error: got %s, wanted %s\n", err, c.err)
			}
		})
	}
}

func generateArrayHeader(numDim, containsNull, elemOID, arrayLength uint32, addData []byte) []byte {
	result := make([]byte, 20)

	binary.BigEndian.PutUint32(result, numDim)
	binary.BigEndian.PutUint32(result[4:], containsNull)
	binary.BigEndian.PutUint32(result[8:], elemOID)
	binary.BigEndian.PutUint32(result[12:], arrayLength)
	binary.BigEndian.PutUint32(result[16:], 0) // filler for upper bound

	return append(result, addData...)
}

func TestPgxSeriesSet(t *testing.T) {
	testCases := []struct {
		name string
		rows []mockPgxRows
	}{
		{
			name: "empty rows",
			rows: make([]mockPgxRows, 0),
		},
		{
			name: "one row",
			rows: genMockRows([]int{1}),
		},
		{
			name: "two rows",
			rows: genMockRows([]int{1, 2}),
		},
		{
			name: "5 rows",
			rows: genMockRows([]int{1, 2, 3, 4, 5}),
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			p := pgxSeriesSet{rows: genPgxRows(c.rows)}

			seriesLen := 0
			for i := range c.rows {
				seriesLen += len(c.rows[i].results)
			}

			for seriesLen > 0 {
				seriesLen--
				if !p.Next() {
					t.Fatal("unexpected end of series set")
				}

				s := p.At()

				var ss *pgxSeries
				var ok bool

				if ss, ok = s.(*pgxSeries); !ok {
					t.Fatal("unexpected type for storage.Series")
				}

				if !reflect.DeepEqual(ss.rows, p.rows[p.rowIdx].RawValues()) {
					t.Fatal("generated series does not match the input")
				}

				if err := p.Err(); err != nil {
					t.Fatalf("unexpected error returned: %s", err)
				}
			}

			if p.Next() {
				t.Fatal("unexpected presence of next row after all rows were iterated on")
			}
			if p.Next() {
				t.Fatal("unexpected presence of next row after all rows were iterated on")
			}
			if p.Next() {
				t.Fatal("unexpected presence of next row after all rows were iterated on")
			}

		})
	}
}

func genMockRows(rowCounts []int) []mockPgxRows {
	result := make([]mockPgxRows, len(rowCounts))

	for i := range result {
		result[i] = genRows(rowCounts[i])
	}

	return result
}

func genRows(count int) mockPgxRows {
	result := mockPgxRows{
		results: make([][][]byte, count),
	}

	for i := range result.results {
		result.results[i] = make([][]byte, 4)

		for j := range result.results[i] {
			result.results[i][j] = []byte(fmt.Sprintf("payload %d %d", i, j))
		}
	}

	return result
}

func genPgxRows(m []mockPgxRows) []pgx.Rows {
	result := make([]pgx.Rows, len(m))

	for i := range result {
		result[i] = &m[i]
	}

	return result
}

func genSeries(labels map[string]string, ts []uint64, vs []float64) [][]byte {
	result := make([][]byte, 4)
	keys := make([]byte, 0, 100)
	values := make([]byte, 0, 100)
	kIdx, vIdx := 0, 0
	for k, v := range labels {
		keyLen := len(k)
		keys = append(keys, []byte("1234")...)
		binary.BigEndian.PutUint32(keys[kIdx:], uint32(keyLen))
		kIdx += 4
		keys = append(keys, k...)
		kIdx += len(k)

		valLen := len(v)
		values = append(values, []byte("1234")...)
		binary.BigEndian.PutUint32(values[vIdx:], uint32(valLen))
		vIdx += 4
		values = append(values, v...)
		vIdx += len(v)
	}

	tts := make([]byte, len(ts)*12)
	vvs := make([]byte, len(vs)*12)
	tIdx, vIdx := 0, 0
	for i, t := range ts {
		binary.BigEndian.PutUint32(tts[tIdx:], 4)
		tIdx += 4
		binary.BigEndian.PutUint64(tts[tIdx:], t)
		tIdx += 8

		binary.BigEndian.PutUint32(vvs[vIdx:], 4)
		vIdx += 4
		binary.BigEndian.PutUint64(vvs[vIdx:], math.Float64bits(vs[i]))
		vIdx += 8
	}
	result[0] = generateArrayHeader(1, 0, 0, uint32(len(labels)), keys)
	result[1] = generateArrayHeader(1, 0, 0, uint32(len(labels)), values)
	result[2] = generateArrayHeader(1, 0, 0, uint32(len(ts)), tts)
	result[3] = generateArrayHeader(1, 0, 0, uint32(len(ts)), vvs)
	return result
}

func makeRawBytes(s []string) [][]byte {
	result := make([][]byte, len(s))

	for i := range result {
		result[i] = []byte(s[i])
	}

	return result
}

func TestPgxSeries(t *testing.T) {
	testCases := []struct {
		name     string
		rawInput []string
		labels   map[string]string
		ts       []uint64
		vs       []float64
		err      error
	}{
		{
			name: "empty rows",
			err:  errInvalidData,
		},
		{
			name:     "invalid keys",
			rawInput: []string{"invalid", "data", "foo", "bar"},
			err:      errHeaderTooShort,
		},
		{
			name:     "invalid label values",
			rawInput: []string{string(generateArrayHeader(1, 0, 0, 2, []byte{})), "data", "foo", "bar"},
			err:      errHeaderTooShort,
		},
		{
			name: "keys and values len mismatch",
			rawInput: []string{
				string(generateArrayHeader(1, 0, 0, 1, []byte{})),
				string(generateArrayHeader(1, 0, 0, 2, []byte{})),
				"foo",
				"bar",
			},
			err: errKeysAndValuesMismatch,
		},
		{
			name: "invalid timestamps",
			rawInput: []string{
				string(generateArrayHeader(1, 1, 0, 0, []byte{})),
				string(generateArrayHeader(1, 1, 0, 0, []byte{})),
				"foo",
				"bar",
			},
			err: errHeaderTooShort,
		},
		{
			name: "invalid series values",
			rawInput: []string{
				string(generateArrayHeader(1, 1, 0, 0, []byte{})),
				string(generateArrayHeader(1, 1, 0, 0, []byte{})),
				string(generateArrayHeader(1, 0, 0, 1, []byte{})),
				"bar",
			},
			err: errHeaderTooShort,
		},
		{
			name: "invalid series values",
			rawInput: []string{
				string(generateArrayHeader(1, 1, 0, 0, []byte{})),
				string(generateArrayHeader(1, 1, 0, 0, []byte{})),
				string(generateArrayHeader(1, 0, 0, 1, []byte{})),
				string(generateArrayHeader(1, 0, 0, 2, []byte{})),
			},
			err: errIterInvalidData,
		},
		{
			name:   "labels set",
			labels: map[string]string{"foo": "bar", "key": "value"},
		},
		{
			name:   "series set",
			labels: map[string]string{"foo": "bar", "key": "value"},
			ts:     []uint64{10000, 55000},
			vs:     []float64{2, 90},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			rows := make([][]byte, 0)
			switch {
			case c.rawInput != nil:
				rows = makeRawBytes(c.rawInput)
			case c.labels != nil:
				rows = genSeries(c.labels, c.ts, c.vs)
			}
			p := pgxSeries{
				rows: rows,
				err:  c.err,
			}

			ll := p.Labels()

			if len(ll) != len(c.labels) {
				t.Errorf("unexpected len of labels: got %d, wanted %d", len(ll), len(c.labels))
			}

			for i := range ll {
				if wanted := c.labels[ll[i].Name]; wanted != ll[i].Value {
					t.Errorf("incorrect label for name %s: got %s, wanted %s\n", ll[i].Name, ll[i].Value, wanted)
				}
			}

			iter := p.Iterator()

			if err := iter.Err(); err != c.err {
				t.Fatalf("unexpected error output: got %s, wanted %s\n", err, c.err)
			}

			resTs := make([]uint64, len(c.ts))
			resVs := make([]float64, len(c.vs))

			for i := range c.ts {
				if !iter.Next() {
					t.Fatalf("unexpected end of iterator")
				}

				ts, val := iter.At()

				resTs[i] = uint64((ts * 1000) - microsecFromUnixEpochToY2K)
				resVs[i] = val
			}

			if len(resTs) > 0 && (!reflect.DeepEqual(resTs, c.ts) || !reflect.DeepEqual(resVs, c.vs)) {
				t.Fatalf("series and input do not match:\ngot:\n%+v %v+\nwanted\n%+v %+v\n", resTs, resVs, c.ts, c.vs)
			}

			//At this point, iterator is exhausted, Next should return false and At zero values.
			if iter.Next() {
				t.Errorf("unexpected next iteration on exhausted iterator")
			}

			if ts, v := iter.At(); ts != 0 && v != 0 {
				t.Errorf("unexpected values from exhausted iterator: got ts: %d, val: %f\n", ts, v)
			}

			if iter.Seek(int64(^uint(0)>>1)) == true {
				t.Errorf("found unexpected timestamp maxInt64")
			}

			if len(c.ts) > 0 {
				for i, ts := range c.ts {
					if !iter.Seek(int64((ts + microsecFromUnixEpochToY2K) / 1000)) {
						t.Errorf("expected Seek to find provided timestamp")
						continue
					}

					if _, val := iter.At(); val != c.vs[i] {
						t.Errorf("incorrect value found: got %f, wanted %f\n", val, c.vs[i])
					}
				}
			}
		})
	}

}
