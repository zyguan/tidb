package xapi

import (
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/xapi/tipb"
)

// SelectResult is used to get response rows from SelectRequest.
type SelectResult struct {
}

func (r *SelectResult) Next() (row [][]byte, err error) {
	return nil
}

func Select(client kv.Client, req *tipb.SelectRequest, concurrency int) *SelectResult {

	return nil
}

func SupportExpression(client kv.Client, expr *tipb.Expression) bool {
	return false
}
