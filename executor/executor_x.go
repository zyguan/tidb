package executor

import (
	"sort"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/optimizer/plan"
	"github.com/pingcap/tidb/xapi"
	"github.com/pingcap/tidb/xapi/tipb"
)

type XSelectTableExec struct {
	tablePlan *plan.TableScan
	where     *tipb.Expression
	ctx       context.Context
	result    *xapi.SelectResult
}

func (e *XSelectTableExec) Next() (*Row, error) {
	if e.result == nil {
		err := e.doRequest()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	resultRow, err := e.result.Next()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resultRowToRow(resultRow), nil
}

func (e *XSelectTableExec) Fields() []*ast.ResultField {
	return e.tablePlan.Fields()
}

func (e *XSelectTableExec) Close() error {
	return nil
}

func resultRowToRow(resultRow [][]byte) *Row {
	return nil
}

func (e *XSelectTableExec) doRequest() error {
	txn, err := e.ctx.GetTxn(false)
	if err != nil {
		return errors.Trace(err)
	}
	selReq := new(tipb.SelectRequest)
	startTs := txn.StartTS()
	selReq.StartTs = &startTs
	selReq.Fields = resultFieldsToPBExpression(e.tablePlan.Fields())
	selReq.Where = conditionsToPBExpression(e.tablePlan.FilterConditions...)
	selReq.Ranges, selReq.Points = tableRangeToPBRangesAndPoints(e.tablePlan.Ranges)
	selReq.TableInfo = tableInfoToPBTableInfo(e.tablePlan.Table)
	e.result, err = xapi.Select(txn.GetClient(), selReq, 1)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

type XSelectIndexExec struct {
	indexPlan *plan.IndexScan
	ctx       context.Context
	where     *tipb.Expression
	rows      []*Row
	cursor    int
}

func (e *XSelectIndexExec) Fields() []*ast.ResultField {
	return e.indexPlan.Fields()
}

func (e *XSelectIndexExec) Next() (*Row, error) {
	if e.rows == nil {
		err := e.doRequest()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if e.cursor >= len(e.rows) {
		return nil, nil
	}
	row := e.rows[e.cursor]
	e.cursor++
	return row, nil
}

func (e *XSelectIndexExec) doRequest() error {
	txn, err := e.ctx.GetTxn(false)
	if err != nil {
		return errors.Trace(err)
	}
	idxResult, err := e.doIndexRequest(txn)
	if err != nil {
		return errors.Trace(err)
	}
	handles, err := extractHandlesFromIndexResult(idxResult)
	if err != nil {
		return errors.Trace(err)
	}
	indexOrder := make(map[int64]int)
	for i, h := range handles {
		indexOrder[h] = i
	}
	sort.Sort(Int64Slice(handles))
	tblResult, err := e.doTableRequest(txn, handles)
	rawRows, err := extractRawRowsFromTableResult(tblResult)
	if err != nil {
		return errors.Trace(err)
	}
	// Restore the original index order.
	rows := make([]*Row, len(handles))
	for i, h := range handles {
		oi := indexOrder[h]
		rows[oi] = rawRowToRow(h, rawRows[i])
	}
	e.rows = rows
	return nil
}

func (e *XSelectIndexExec) doIndexRequest(txn kv.Transaction) (*xapi.SelectResult, error) {
	selIdxReq := new(tipb.SelectRequest)
	startTs := txn.StartTS()
	selIdxReq.StartTs = &startTs
	selIdxReq.IndexInfo = indexInfoToPBIndexInfo(e.indexPlan.Index)
	selIdxReq.Ranges, selIdxReq.Points = indexRangesToPBRangesAndPoints(e.indexPlan.Ranges)
	return xapi.Select(txn.GetClient(), selIdxReq, 1)
}

func (e *XSelectIndexExec) doTableRequest(txn kv.Transaction, handles []int64) (*xapi.SelectResult, error) {
	selTableReq := new(tipb.SelectRequest)
	startTs := txn.StartTS()
	selTableReq.StartTs = &startTs
	selTableReq.TableInfo = tableInfoToPBTableInfo(e.indexPlan.Table)
	selTableReq.Fields = resultFieldsToPBExpression(e.indexPlan.Fields())
	selTableReq.Points = handlesToPoints(handles)
	selTableReq.Where = conditionsToPBExpression(e.indexPlan.FilterConditions)
	return xapi.Select(txn.GetClient(), selTableReq, 10)
}

func conditionsToPBExpression(expr ...ast.ExprNode) *tipb.Expression {
	return nil
}

func resultFieldsToPBExpression(fields []*ast.ResultField) []*tipb.Expression {
	return nil
}

func tableRangeToPBRangesAndPoints(tableRanges []plan.TableRange) ([]*tipb.KeyRange, []*tipb.KeyPoint) {
	return nil, nil
}

func tableInfoToPBTableInfo(tbInfo *model.TableInfo) *tipb.TableInfo {
	return nil
}

func indexInfoToPBIndexInfo(idxInfo *model.IndexInfo) *tipb.IndexInfo {
	return nil
}

func indexRangesToPBRangesAndPoints(ranges []*plan.IndexRange) ([]*tipb.KeyRange, []*tipb.KeyPoint) {
	return nil, nil
}

func extractHandlesFromIndexResult(idxResult *xapi.SelectResult) []int64 {
	return nil
}

func extractRawRowsFromTableResult(tblResult *xapi.SelectResult) []*tipb.Row {
	return nil
}

func handlesToPoints(handles []int64) []*tipb.KeyPoint {
	return nil
}

func rawRowToRow(h int64, row [][]byte) *Row {
	return nil
}

type Int64Slice []int64

func (p Int64Slice) Len() int           { return len(p) }
func (p Int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
