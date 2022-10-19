// Copyright 2022 PingCAP, Inc.
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

package tablecache

import (
	"context"
	"sync"

	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tipb/go-tipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var (
	lock      sync.Mutex
	localAddr string
)

func SetLocalAddr(addr string) {
	lock.Lock()
	localAddr = addr
	lock.Unlock()
}

type TableIndex interface {
	Get(ts uint64, id int64) (table.Table, error)
}

func RegisterServer(s *grpc.Server, idx TableIndex) {
	tipb.RegisterTableCacheAgentServer(s, &server{idx})
}

type server struct {
	idx TableIndex
}

func (s *server) Invalidate(ctx context.Context, req *tipb.InvalidateRequest) (*tipb.InvalidateResponse, error) {
	tid, minReadLease := req.GetTableId(), req.GetMinReadLease()
	tbl, err := s.idx.Get(minReadLease, tid)
	if err != nil {
		return nil, grpc.Errorf(codes.FailedPrecondition, "failed to get table#%d by ts(%d): %v", tid, minReadLease, err)
	}
	t, ok := tbl.(table.CachedTable)
	if !ok {
		return nil, grpc.Errorf(codes.FailedPrecondition, "table#%d is not cached", tid)
	}
	maxReadTS := t.InvalidateCache(minReadLease)
	return &tipb.InvalidateResponse{MaxReadTs: maxReadTS}, nil
}

func NewClient() (cli *client) {
	lock.Lock()
	cli = &client{addr: localAddr}
	lock.Unlock()
	return
}

type client struct {
	addr string
}

func (c *client) LocalAddr() string { return c.addr }

func (c *client) Invalidate(ctx context.Context, addr string, tid int64, minReadLease uint64) (uint64, error) {
	cc, err := grpc.DialContext(ctx, addr, grpc.WithInsecure())
	if err != nil {
		return 0, err
	}
	defer cc.Close()
	cli := tipb.NewTableCacheAgentClient(cc)
	resp, err := cli.Invalidate(ctx, &tipb.InvalidateRequest{TableId: tid, MinReadLease: minReadLease})
	if err != nil {
		return 0, err
	}
	return resp.GetMaxReadTs(), nil
}
