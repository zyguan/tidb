package xapi

import (
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/xapi/tablecodec"
	"github.com/pingcap/tidb/xapi/tipb"
)

// SelectResult is used to get response rows from SelectRequest.
type SelectResult struct {
}

func (r *SelectResult) Next() (row [][]byte, err error) {
	return nil
}

func Select(client kv.Client, req *tipb.SelectRequest, concurrency int) (*SelectResult, error) {
	// Convert tipb.*Request to kv.Request
	kvReq, err := composeRequest(req, concurrency)
	if err != nil {
		return nil, error.Trace(err)
	}
	repIter := client.Send(kvReq)
	return nil, nil
}

// Convert tipb.Request to kv.Request
func composeRequest(req *tipbSelectRequest, concurrency int) (*kv.Request, error) {
	kvReq := &kv.Request{
		tp:          kv.ReqTypeSelect,
		Concurrency: concurrency,
	}
	keyRanges := make([]*kv.KeyRange, 0, len(req.GetRanges())+len(req.GetPoints))
	// Compose startkey/endkey
	idx := req.GetIndexInfo()
	tbl := req.GetTableInfo()

	// Convert KeyRanges
	for _, r := range req.GetRanges() {
		// Convert range to kv.KeyRange
		start := tablecodec.EncodeRowKey(r.GetLow())
		end := tablecodec.EncodeRowKey(r.GetHigh())
		nr := &kv.KeyRange{
			StartKey: start,
			EndKey:   end,
		}
	}
	// Convert KeyPoints
	for _, p := range req.GetPoints() {
		// Convert KeyPoint to kv.KeyRange
		start := tablecodec.EncodeRowKey(p.Key)
		// TODO: check this?
		//endKey := append(p.Key, []byte{'\0'})
		end := tablecodec.EncodeRowKey(endKey)
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

func encodeKey(key []byte, tbl *tipb.TableInfo, idx *tipb.IndexInfo) kv.Key {
	/*
		if tbl != nil {
			// TODO: convert
			rid := int64(0)
			return tablecodec.EncodeRecordKey(tbl.GetTableId(), rid, 0)
		}
		return tablecodec.EncodeIndexKey(idx.GetTableId(), key)
	*/
	return kv.Key{}
}

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
	//cmp, err := a.value.CompareDatum(b.value)
	cmp := 0
	err := nil
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
