// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"github.com/c4pt0r/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	pb "github.com/pingcap/tidb/store/tikv/kvrpc/proto"
)

var (
	_ kv.Snapshot = (*tikvSnapshot)(nil)
	_ kv.Iterator = (*tikvIter)(nil)
)

const tikvBatchSize = 1000
const tikvScanLimit uint32 = 1000

// tikvSnapshot implements MvccSnapshot interface.
type tikvSnapshot struct {
	store   *tikvStore
	version kv.Version
}

// newTiKVSnapshot creates a snapshot of an TiKV store.
func newTiKVSnapshot(store *tikvStore, ver kv.Version) *tikvSnapshot {
	return &tikvSnapshot{
		store:   store,
		version: ver,
	}
}

// BatchGet implements kv.Snapshot.BatchGet interface.
func (s *tikvSnapshot) BatchGet(keys []kv.Key) (map[string][]byte, error) {
	// TODO: send batch get rpc
	m := make(map[string][]byte, len(keys))
	var err error
	for _, key := range keys {
		k := string(key)
		m[k], err = s.Get(key)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return m, nil
}

// Get gets the value for key k from snapshot.
func (s *tikvSnapshot) Get(k kv.Key) ([]byte, error) {
	// construct CmdGetRequest
	cmdGetReq := &pb.CmdGetRequest{
		Key:     []byte(k),
		Version: proto.Uint64(s.version.Ver),
	}
	req := &pb.Request{
		Type:      pb.MessageType_CmdGet.Enum(),
		CmdGetReq: cmdGetReq,
	}

	resp, err := s.store.conn.Send(req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if resp.GetType() != pb.MessageType_CmdGet {
		return nil, errors.Errorf("receive response[%s] dismatch request[%s]",
			resp.GetType(), req.GetType())
	}
	cmdGetResp := resp.GetCmdGetResp()
	if cmdGetResp == nil {
		return nil, errors.New("body is missing in get response")
	}
	if !cmdGetResp.GetOk() {
		return nil, errors.New("some error occur in Get")
	}
	return cmdGetResp.Value, nil
}

func (s *tikvSnapshot) Seek(k kv.Key) (kv.Iterator, error) {
	// construct CmdScanRequest
	if k == nil {
		k = []byte("")
	}
	cmdScanReq := &pb.CmdScanRequest{
		Key:     []byte(k),
		Limit:   proto.Uint32(tikvScanLimit),
		Version: proto.Uint64(s.version.Ver),
	}
	req := &pb.Request{
		Type:       pb.MessageType_CmdScan.Enum(),
		CmdScanReq: cmdScanReq,
	}

	log.Debugf("start_key %d %s", len(k), k)
	resp, err := s.store.conn.Send(req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if resp.GetType() != pb.MessageType_CmdScan {
		return nil, errors.Errorf("receive response[%s] dismatch request[%s]",
			resp.GetType(), req.GetType())
	}
	cmdScanResp := resp.GetCmdScanResp()
	if cmdScanResp == nil {
		return nil, errors.New("body is missing in scan response")
	}
	if !cmdScanResp.GetOk() {
		return nil, errors.New("some error occur in Scan")
	}
	return newTiKVIter(cmdScanResp.GetResults()), nil
}

func (s *tikvSnapshot) Release() {
}

type tikvIter struct {
	valid bool
	rs    []*pb.KvPair
	idx   int
}

func newTiKVIter(kv []*pb.KvPair) *tikvIter {
	return &tikvIter{
		valid: len(kv) > 0,
		rs:    kv,
		idx:   0,
	}
}

func (it *tikvIter) Next() error {
	if it.valid {
		it.idx++
		if it.idx >= len(it.rs) {
			it.valid = false
			return errors.New("out of tikv iterator range")
		}
		return nil
	}
	return errors.New("tikv iterator is invalid")
}

func (it *tikvIter) Valid() bool {
	return it.valid
}

func (it *tikvIter) Key() kv.Key {
	if !it.valid {
		return kv.Key(nil)
	}
	return kv.Key(it.rs[it.idx].Key)
}

func (it *tikvIter) Value() []byte {
	if !it.valid {
		return nil
	}
	return it.rs[it.idx].Value
}

func (it *tikvIter) Close() {
	it.valid = false
}
