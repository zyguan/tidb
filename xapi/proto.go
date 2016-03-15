package xapi

import (
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/xapi/tipb"
)

func ColumnToProto(c *model.ColumnInfo) *tipb.ColumnInfo {
	pc := &tipb.ColumnInfo{
		ColumnId:  proto.Int64(c.ID),
		Collation: CollationToProto(c.FieldType.Collate),
		ColumnLen: proto.Int32(int32(c.FieldType.Flen)),
		Decimal:   proto.Int32(int32(c.FieldType.Decimal)),
	}
	t := tipb.MysqlType(int32(c.FieldType.Tp))
	pc.Tp = &t
	return pc
}

func CollationToProto(c string) *tipb.Collation {
	v, ok := tipb.Collation_value[c]
	if ok {
		a := tipb.Collation(v)
		return &a
	}
	a := tipb.Collation_utf8_general_ci
	return &a
}

func TableToProto(t *model.TableInfo) *tipb.TableInfo {
	pt := &tipb.TableInfo{
		TableId: proto.Int64(t.ID),
	}
	cols := make([]*tipb.ColumnInfo, 0, len(t.Columns))
	pkOffset := -1
	if t.PKIsHandle {
		for _, idx := range t.Indices {
			if idx.Primary {
				pkOffset = idx.Columns[0].Offset
			}
		}
	}
	for i, c := range t.Columns {
		col := ColumnToProto(c)
		if i == pkOffset {
			col.PkHandle = proto.Bool(true)
		}
		cols = append(cols, col)
	}
	pt.Columns = cols
	return pt
}

func IndexToProto(tid int64, idx *model.IndexInfo) *tipb.IndexInfo {
	/*
		pi := &tipb.IndexInfo{
			TableId: proto.Int64(tid),
			IndexId: proto.Int64(idx.ID),
			Unique:  proto.Bool(idx.Unique),
		}
		cols := make([]*tipb.ColumnInfo, 0, len(idx.Columns))
		for _, c := range t.Columns {
			col := ColumnToProto(c)
			cols = append(cols, c)
		}
		pi.Columns = cols
		return pi
	*/
	return nil
}
