package localstore

import (
	"bytes"
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/xapi/tablecodec"
	"github.com/pingcap/tidb/xapi/tipb"
	"strconv"
)

// local region server.
type localRS struct {
	startKey []byte
	endKey   []byte
}

type regionRequest struct {
	Tp       int64
	data     []byte
	startKey []byte
	endKey   []byte
}

type regionResponse struct {
	req  *regionRequest
	err  error
	data []byte
	// If region missed some request key range, newStartKey and newEndKey is returned.
	newStartKey []byte
	newEndKey   []byte
}

func (rs *localRS) Handle(req *regionRequest) (*regionResponse, error) {
	resp := &regionResponse{
		req: req,
	}
	if req.Tp == kv.ReqTypeSelect {
		sel := new(tipb.SelectRequest)
		err := proto.Unmarshal(req.data, sel)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rows, err := getRowsFromSelectReq(sel)
		selResp := new(tipb.SelectResponse)
		selResp.Error = toPBError(err)
		selResp.Rows = rows
		data, err := proto.Marshal(selResp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		resp.data = data
	}
	if bytes.Compare(rs.startKey, req.startKey) < 0 || bytes.Compare(rs.endKey, req.endKey) > 0 {
		resp.newStartKey = rs.startKey
		resp.newEndKey = rs.endKey
	}
	return resp, nil
}

func getRowsFromSelectReq(sel *tipb.SelectRequest) ([]*tipb.Row, error) {
	tid := sel.TableInfo.TableId
	for _, kran := range sel.Ranges {
		handle, err := parseInt64(kran.Low)
		if err != nil {
			return nil, errors.Trace(err)
		}
		key := tablecodec.EncodeRecordKey(tid, handle)

	}
	return nil, nil
}

func parseInt64(b []byte) (int64, error) {
	return strconv.ParseInt(hack.String(b), 10, 64)
}

func toPBError(err error) *tipb.Error {
	return nil
}
