package xapi

import (
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/xapi/tablecodec"
	"github.com/pingcap/tidb/xapi/tipb"
)

func ColumnToProto(c *model.ColumnInfo) *tipb.ColumnInfo {
	pc := &tipb.ColumnInfo{
		ColumnId:  proto.Int64(c.ID),
		Tp:        proto.Int32(c.FieldType.Tp),
		Collation: CollationToProto(c.FieldType.Collate),
		ColumnLen: proto.Int32(c.FieldType.Flen),
		Decimal:   proto.Int32(c.FieldType.Decimal),
	}
	return pc
}

func CollationToProto(c string) *tipb.Collation {
	v, ok := tipb.Collation_value[c]
	if ok {
		return v
	}
	return tipb.Collation_utf8_general_ci
}

func TableToProto(t *model.TableInfo) *tipb.TableInfo {
	pt := &tipb.TableInfo{
		TableId: proto.Int64(t.ID),
	}
	cols := make([]*tipb.ColumnInfo, 0, len(t.Columns))
	for _, c := range t.Columns {
		col := ColumnToProto(c)
		// TODO: Set PkHandle
		cols = append(cols, c)
	}
	pt.Columns = cols
	return pt
}

func IndexToProto(tid int64, idx *model.IndexInfo) *tipb.IndexInfo {
	pi := &tipb.IndexInfo{
		TableId: proto.Int64(tid),
		IndexId: proto.Int64(idx.ID),
		Unique:  proto.Bool(idx.Unique),
	}
	cols := make([]*tipb.ColumnInfo, 0, len(t.Columns))
	for _, c := range t.Columns {
		col := ColumnToProto(c)
		cols = append(cols, c)
	}
	pi.Columns = cols
	return pi
}
