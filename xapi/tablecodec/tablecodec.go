package tablecodec

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

var (
	TablePrefix     = []byte{'t'}
	recordPrefixSep = []byte("_r")
	indexPrefixSep  = []byte("_i")
)

func EncodeRecordKey(tableId int64, h int64, columnID int64) kv.Key {
	recordPrefix := genTableRecordPrefix(tableId)
	buf := make([]byte, 0, len(recordPrefix)+16)
	buf = append(buf, recordPrefix...)
	buf = codec.EncodeInt(buf, h)
	if columnID != 0 {
		buf = codec.EncodeInt(buf, columnID)
	}
	return buf
}

func EncodeRowKey(tableId int64, bsHandle []byte) kv.Key {
	recordPrefix := genTableRecordPrefix(tableId)
	key := make([]byte, 0, len(recordPrefix)+16)
	key = append(key, recordPrefix...)
	key = codec.EncodeKey(key, bsHandle)
	return key
}

func EncodeIndexRangeKey(tableId int64, key []byte) (kv.Key, error) {
	prefix := genTableIndexPrefix(tableId)
	key = append(key, prefix...)
	if distinct {
		key, err = codec.EncodeKey(key, indexedValues...)
	} else {
		key, err = codec.EncodeKey(key, append(indexedValues, types.NewDatum(handle))...)
	}
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	return key, distinct, nil
}

func EncodeIndexKey(tableId int64, indexedValues []types.Datum, handle int64, unique bool) (key []byte, distinct bool, err error) {
	if unique {
		// See: https://dev.mysql.com/doc/refman/5.7/en/create-index.html
		// A UNIQUE index creates a constraint such that all values in the index must be distinct.
		// An error occurs if you try to add a new row with a key value that matches an existing row.
		// For all engines, a UNIQUE index permits multiple NULL values for columns that can contain NULL.
		distinct = true
		for _, cv := range indexedValues {
			if cv.Kind() == types.KindNull {
				distinct = false
				break
			}
		}
	}
	prefix := genTableIndexPrefix(tableId)
	key = append(key, prefix...)
	if distinct {
		key, err = codec.EncodeKey(key, indexedValues...)
	} else {
		key, err = codec.EncodeKey(key, append(indexedValues, types.NewDatum(handle))...)
	}
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	return key, distinct, nil
}

// record prefix is "t[tableID]_r"
func genTableRecordPrefix(tableID int64) kv.Key {
	buf := make([]byte, 0, len(TablePrefix)+8+len(recordPrefixSep))
	buf = append(buf, TablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, recordPrefixSep...)
	return buf
}

// index prefix is "t[tableID]_i"
func genTableIndexPrefix(tableID int64) kv.Key {
	buf := make([]byte, 0, len(TablePrefix)+8+len(indexPrefixSep))
	buf = append(buf, TablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, indexPrefixSep...)
	return buf
}
