package xapi

import (
	"io"
	"io/ioutil"
	"sort"

	"github.com/gogo/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/xapi/tablecodec"
	"github.com/pingcap/tidb/xapi/tipb"
)

// SelectResult is used to get response rows from SelectRequest.
type SelectResult struct {
	iter     kv.ResponseIterator
	resp     *tipb.SelectResponse
	cursor   int
	finished bool
}

func (r *SelectResult) Next() (handle int64, data []types.Datum, err error) {
	if r.finished {
		return 0, nil, nil
	}
	for {
		if r.resp == nil {
			var reader io.ReadCloser
			reader, err = r.iter.Next()
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
			if reader == nil {
				r.finished = true
				return 0, nil, nil
			}
			var b []byte
			b, err = ioutil.ReadAll(reader)
			reader.Close()
			resp := new(tipb.SelectResponse)
			err = proto.Unmarshal(b, resp)
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
			r.resp = resp
		}
		if r.cursor >= len(r.resp.Rows) {
			r.resp = nil
			r.cursor = 0
			continue
		}
		row := r.resp.Rows[r.cursor]
		data, err = codec.Decode(row.Data)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		r.cursor++
		return row.GetHandle(), data, nil
	}
}

func (r *SelectResult) Close() error {
	return nil
}

func Select(client kv.Client, req *tipb.SelectRequest, concurrency int) (*SelectResult, error) {
	// Convert tipb.*Request to kv.Request
	kvReq, err := composeRequest(req, concurrency)
	if err != nil {
		return nil, errors.Trace(err)
	}
	repIter := client.Send(kvReq)
	return &SelectResult{iter: repIter}, nil
}

func SelectIndex(client kv.Client, req *tipb.IndexRequest, concurrency int) (*SelectResult, error) {
	return nil, nil
}

// Convert tipb.Request to kv.Request
func composeRequest(req *tipb.SelectRequest, concurrency int) (*kv.Request, error) {
	kvReq := &kv.Request{
		Tp:          kv.ReqTypeSelect,
		Concurrency: concurrency,
	}
	keyRanges := make([]kv.KeyRange, 0, len(req.GetRanges())+len(req.GetPoints()))
	// Compose startkey/endkey
	tbl := req.GetTableInfo()
	tid := tbl.GetTableId()
	// Convert KeyRanges
	for _, r := range req.GetRanges() {
		// Convert range to kv.KeyRange
		start := tablecodec.EncodeRecordKey(tid, r.GetLow(), 0)
		end := tablecodec.EncodeRecordKey(tid, r.GetHigh()+1, 0)
		nr := kv.KeyRange{
			StartKey: start,
			EndKey:   end,
		}
		keyRanges = append(keyRanges, nr)
	}
	// Convert KeyPoints
	for _, p := range req.GetPoints() {
		// Convert KeyPoint to kv.KeyRange
		start := tablecodec.EncodeRecordKey(tid, p, 0)
		end := tablecodec.EncodeRecordKey(tid, p+1, 0)
		nr := kv.KeyRange{
			StartKey: start,
			EndKey:   end,
		}
		keyRanges = append(keyRanges, nr)
	}
	// Sort KeyRanges
	sorter := keyRangeSorter{ranges: keyRanges}
	sort.Sort(&sorter)
	if sorter.err != nil {
		return nil, errors.Trace(sorter.err)
	}
	kvReq.KeyRanges = sorter.ranges
	return kvReq, nil
}

// Sort KeyRange
type keyRangeSorter struct {
	ranges []kv.KeyRange
	err    error
}

func (r *keyRangeSorter) Len() int {
	return len(r.ranges)
}

func (r *keyRangeSorter) Less(i, j int) bool {
	a := r.ranges[i]
	b := r.ranges[j]
	cmp, err := types.Compare(a, b)
	if err != nil {
		r.err = err
		return true
	}
	return cmp <= 0
}

func (r *keyRangeSorter) Swap(i, j int) {
	r.ranges[i], r.ranges[j] = r.ranges[j], r.ranges[i]
}

func SupportExpression(client kv.Client, expr *tipb.Expression) bool {
	return false
}
