// Copyright 2016 PingCAP, Inc.
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
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	pb "github.com/pingcap/tidb/store/tikv/kvrpc/proto"
)

var (
	_ kv.Transaction = (*tikvTxn)(nil)
)

type tikvTxn struct {
	us        kv.UnionStore
	store     *tikvStore // for commit
	storeName string
	tid       uint64
	valid     bool
	version   kv.Version // commit version
	lockKeys  [][]byte
}

func newTiKVTxn(store *tikvStore, storeName string) *tikvTxn {
	startTS, _ := store.oracle.GetTimestamp()
	ver := kv.NewVersion(startTS)
	return &tikvTxn{
		us:        kv.NewUnionStore(newTiKVSnapshot(store, ver)),
		store:     store,
		storeName: storeName,
		tid:       startTS,
		valid:     true,
	}
}

// Implement transaction interface
func (txn *tikvTxn) Get(k kv.Key) ([]byte, error) {
	log.Debugf("[kv] get key:%q, txn:%d", k, txn.tid)
	return txn.us.Get(k)
}

func (txn *tikvTxn) Set(k kv.Key, v []byte) error {
	log.Debugf("[kv] set %q txn:%d", k, txn.tid)
	return txn.us.Set(k, v)
}

func (txn *tikvTxn) String() string {
	return fmt.Sprintf("%d", txn.tid)
}

func (txn *tikvTxn) Seek(k kv.Key) (kv.Iterator, error) {
	log.Debugf("[kv] seek %q txn:%d", k, txn.tid)
	return txn.us.Seek(k)
}

func (txn *tikvTxn) Delete(k kv.Key) error {
	log.Debugf("[kv] delete %q txn:%d", k, txn.tid)
	return txn.us.Delete(k)
}

func (txn *tikvTxn) SetOption(opt kv.Option, val interface{}) {
	txn.us.SetOption(opt, val)
}

func (txn *tikvTxn) DelOption(opt kv.Option) {
	txn.us.DelOption(opt)
}

// doCommit First invoke prewrite, if it is ok then invoke commit
func (txn *tikvTxn) doCommit() error {
	if err := txn.us.CheckLazyConditionPairs(); err != nil {
		return errors.Trace(err)
	}

	// Construct CmdPrewriteRequest.
	cmdPrewriteReq := new(pb.CmdPrewriteRequest)
	err := txn.us.WalkBuffer(func(k kv.Key, v []byte) error {
		// Convert Key to byte[].
		row := append([]byte(nil), k...)
		if len(v) == 0 { // Deleted marker
			cmdPrewriteReq.Dels = append(cmdPrewriteReq.Dels, row)
		} else {
			val := append([]byte(nil), v...)
			kv := pb.KvPair{Key: row, Value: val}
			cmdPrewriteReq.Puts = append(cmdPrewriteReq.Puts, &kv)
		}
		return nil
	})
	cmdPrewriteReq.Locks = txn.lockKeys
	startTS, err := txn.store.oracle.GetTimestamp()
	if err != nil {
		return errors.Trace(err)
	}
	cmdPrewriteReq.StartVersion = proto.Uint64(startTS)
	req := &pb.Request{
		Type:           pb.MessageType_CmdPrewrite.Enum(),
		CmdPrewriteReq: cmdPrewriteReq,
	}
	txn.version = kv.Version{Ver: startTS}

	// Receive prewrite response, then call commit.
	resp, err := txn.store.conn.Send(req)
	if err != nil {
		return errors.Trace(err)
	}
	if resp.GetType() != pb.MessageType_CmdPrewrite {
		return errors.Errorf("receive response[%s] dismatch request[%s]",
			resp.GetType(), req.GetType())
	}
	cmdPrewriteResp := resp.GetCmdPrewriteResp()
	if cmdPrewriteResp == nil {
		return errors.New("body is missing in prewrite response")
	}
	if !cmdPrewriteResp.GetOk() {
		return errors.New("some error occur in Prewrite")
	}

	// Construct CmdCommitRequest.
	cmdCommitReq := new(pb.CmdCommitRequest)
	commitTS, err := txn.store.oracle.GetTimestamp()
	if err != nil {
		return errors.Trace(err)
	}
	cmdCommitReq.StartVersion = proto.Uint64(startTS)
	cmdCommitReq.CommitVersion = proto.Uint64(commitTS)
	commitReq := &pb.Request{
		Type:         pb.MessageType_CmdCommit.Enum(),
		CmdCommitReq: cmdCommitReq,
	}

	// Receive commit response.
	commitResp, err := txn.store.conn.Send(commitReq)
	if err != nil {
		return errors.Trace(err)
	}
	if commitResp.GetType() != pb.MessageType_CmdCommit {
		return errors.Errorf("receive response[%s] dismatch request[%s]",
			commitResp.GetType(), commitReq.GetType())
	}
	cmdCommitResp := commitResp.GetCmdCommitResp()
	if cmdCommitResp == nil {
		return errors.New("body is missing in commit response")
	}
	if !cmdCommitResp.GetOk() {
		return errors.New("some error occur in Commit")
	}
	return nil
}

func (txn *tikvTxn) Commit() error {
	if !txn.valid {
		return kv.ErrInvalidTxn
	}
	log.Debugf("[kv] start to commit txn %d", txn.tid)
	defer func() {
		txn.close()
	}()
	return txn.doCommit()
}

func (txn *tikvTxn) close() error {
	txn.us.Release()
	txn.valid = false
	return nil
}

func (txn *tikvTxn) Rollback() error {
	if !txn.valid {
		return kv.ErrInvalidTxn
	}
	log.Warnf("[kv] Rollback txn %d", txn.tid)
	return nil
}

func (txn *tikvTxn) LockKeys(keys ...kv.Key) error {
	// TODO: impl this
	for _, key := range keys {
		txn.lockKeys = append(txn.lockKeys, key)
	}
	return nil
}
