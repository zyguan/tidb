package localstore

import (
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/kv/tipb"
)

type localXAPI struct {
}

func (x *localXAPI) Select(req *tipb.SelectRequest, concurrency int) kv.SelectResult {

	return nil
}

func (x *localXAPI) IsSupported(expr *tipb.Expression) bool {
	return false
}

func encodeKey()
