// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package context

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
)

// RangerContext is the context used to build range.
type RangerContext struct {
	TypeCtx types.Context
	ErrCtx  errctx.Context
	ExprCtx exprctx.BuildContext
	*contextutil.RangeFallbackHandler
	*contextutil.PlanCacheTracker
	*PointAllocator
	OptimizerFixControl      map[uint64]string
	UseCache                 bool
	InPreparedPlanBuilding   bool
	RegardNULLAsPoint        bool
	OptPrefixIndexSingleScan bool
}

// Detach detaches this context from the session context.
//
// NOTE: Though this session context can be used parallelly with this context after calling
// it, the `StatementContext` cannot. The session context should create a new `StatementContext`
// before executing another statement.
func (r *RangerContext) Detach(staticExprCtx exprctx.BuildContext) *RangerContext {
	newCtx := *r
	newCtx.ExprCtx = staticExprCtx
	newCtx.OptimizerFixControl = make(map[uint64]string, len(r.OptimizerFixControl))
	for k, v := range r.OptimizerFixControl {
		newCtx.OptimizerFixControl[k] = v
	}
	return &newCtx
}

// PointAllocator is used to allocate points.
type PointAllocator struct {
	data []Point
}

// Allocate allocates a point.
func (a *PointAllocator) Allocate(val Point) *Point {
	a.data = append(a.data, val)
	return &a.data[len(a.data)-1]
}

// Reset keeps n allocated points and releases others for reusing.
func (a *PointAllocator) Reset(n int) {
	a.data = a.data[:n]
}

// Size returns the size of the allocated points.
func (a *PointAllocator) Size() int {
	return len(a.data)
}

// Point is the end Point of range interval.
type Point struct {
	Value types.Datum
	Excl  bool // exclude
	Start bool
}

// String implements fmt.Stringer interface.
func (p *Point) String() string {
	val := p.Value.GetValue()
	if p.Value.Kind() == types.KindMinNotNull {
		val = "-inf"
	} else if p.Value.Kind() == types.KindMaxValue {
		val = "+inf"
	}
	if p.Start {
		symbol := "["
		if p.Excl {
			symbol = "("
		}
		return fmt.Sprintf("%s%v", symbol, val)
	}
	symbol := "]"
	if p.Excl {
		symbol = ")"
	}
	return fmt.Sprintf("%v%s", val, symbol)
}
