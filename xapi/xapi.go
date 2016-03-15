package xapi

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/xapi/tablecodec"
	"github.com/pingcap/tidb/xapi/tipb"
)

// SelectResult is used to get response rows from SelectRequest.
type SelectResult struct {
	iter     kv.ResponseIterator
	resp     *kv.Response
	finished bool
}

func (r *SelectResult) Next() ([]byte, error) {
	if r.finished {
		return nil, nil
	}
	for {
		var err error
		if r.resp == nil {
			r.resp, err = r.iter.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if r.resp == nil {
				r.finished = true
				return nil, nil
			}
		}
		row, err := r.resp.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row == nil {
			r.resp = nil
			continue
		}
		return row, err
	}
	return nil, nil
}

func Select(client kv.Client, req *tipb.SelectRequest, concurrency int) (*SelectResult, error) {
	// Convert tipb.*Request to kv.Request
	kvReq, err := composeRequest(req, concurrency)
	if err != nil {
		return nil, error.Trace(err)
	}
	repIter := client.Send(kvReq)
	return &SelectResult{iter: repIter}, nil
}

// Convert tipb.Request to kv.Request
func composeRequest(req *tipbSelectRequest, concurrency int) (*kv.Request, error) {
	kvReq := &kv.Request{
		tp:          kv.ReqTypeSelect,
		Concurrency: concurrency,
	}
	keyRanges := make([]*kv.KeyRange, 0, len(req.GetRanges())+len(req.GetPoints))
	// Compose startkey/endkey
	tbl := req.GetTableInfo()

	// Convert KeyRanges
	for _, r := range req.GetRanges() {
		// Convert range to kv.KeyRange
		start := tablecodec.EncodeRecordKey(r.GetLow())
		end := tablecodec.EncodeRecordKey(r.GetHigh() + 1)
		nr := &kv.KeyRange{
			StartKey: start,
			EndKey:   end,
		}
	}
	// Convert KeyPoints
	for _, p := range req.GetPoints() {
		// Convert KeyPoint to kv.KeyRange
		start := tablecodec.EncodeRecordKey(p)
		end := tablecodec.EncodeRecordKey(p + 1)
		nr := &kv.KeyRange{
			StartKey: start,
			EndKey:   end,
		}
	}
	// Sort KeyRanges
	sorter := keyRangeSorter{ranges: keyRanges}
	sort.Sort(&sorter)
	if sorter.err != nil {
		return nil, errors.Trace(err)
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
