package tablecodec

import (
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/codec"
)

func EncodeColumnKey(tableId int64, h int64, columnID int64) kv.Key {
	buf := make([]byte, 0, len(recordPrefix)+16)
	buf = append(buf, recordPrefix...)
	buf = codec.EncodeInt(buf, h)

	if columnID != 0 {
		buf = codec.EncodeInt(buf, columnID)
	}
	return buf
}

func EncodeRecordKey(tableId int64, handle int64)
