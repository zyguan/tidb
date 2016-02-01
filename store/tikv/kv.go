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
	"math/rand"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/oracle/oracles"
)

const (
	// fix length conn pool
	tikvConnPoolSize = 10
)

var (
	// ErrInvalidDSN is returned when store dsn is invalid.
	ErrInvalidDSN = errors.New("invalid tikv dsn")
)

type storeCache struct {
	mu    sync.Mutex
	cache map[string]*tikvStore
}

var mc storeCache

// Driver implements engine Driver.
type Driver struct {
}

const (
	defaultKVPort = 61234
)

// Open opens or creates an TiKV storage with given path.
//
// The format of path should be 'tikv://ip:port/table[?tso=tso_ip:tso_port]'.
// If tso is not provided, it will use as server. (for test only)
func (d Driver) Open(path string) (kv.Storage, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	srvHost, tsoHost, tableName, err := parsePath(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	uuid := fmt.Sprintf("tikv-%v-%v-%v", srvHost, tsoHost, tableName)
	if store, ok := mc.cache[uuid]; ok {
		return store, nil
	}

	var ora oracle.Oracle
	ora = oracles.NewRemoteOracle(tsoHost)

	s := &tikvStore{
		uuid:      uuid,
		storeName: tableName,
		oracle:    ora,
		conn:      NewClient(srvHost),
	}
	mc.cache[uuid] = s
	return s, nil
}

type tikvStore struct {
	mu        sync.Mutex
	uuid      string
	storeName string
	oracle    oracle.Oracle
	conn      *Client
}

func (s *tikvStore) getTiKVClient() *Client {
	return s.conn
}

func (s *tikvStore) Begin() (kv.Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return newTiKVTxn(s, s.storeName), nil
}

func (s *tikvStore) GetSnapshot(ver kv.Version) (kv.Snapshot, error) {
	txn := newTiKVTxn(s, s.storeName)
	txn.version = ver
	snapshot := newTiKVSnapshot(s, ver)
	return snapshot, nil
}

func (s *tikvStore) Close() error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	delete(mc.cache, s.uuid)

	var err error
	err = s.conn.Close()
	if err != nil {
		log.Error(err)
	}
	return err
}

func (s *tikvStore) UUID() string {
	return s.uuid
}

func (s *tikvStore) CurrentVersion() (kv.Version, error) {
	startTS, err := s.oracle.GetTimestamp()
	if err != nil {
		return kv.NewVersion(0), errors.Trace(err)
	}
	return kv.NewVersion(startTS), nil
}

func parsePath(path string) (srvHost string, tsoHost string, tableName string, err error) {
	u, err := url.Parse(path)
	if err != nil {
		return "", "", "", errors.Trace(err)
	}
	if strings.ToLower(u.Scheme) != "tikv" {
		return "", "", "", errors.Trace(ErrInvalidDSN)
	}
	p, tableName := filepath.Split(u.Path)
	if p != "/" {
		return "", "", "", errors.Trace(ErrInvalidDSN)
	}
	srvHost = u.Host
	tsoHost = u.Query().Get("tso")
	if tsoHost == "" {
		tsoHost = srvHost
	}
	return srvHost, tsoHost, tableName, nil
}

func init() {
	mc.cache = make(map[string]*tikvStore)
	rand.Seed(time.Now().UnixNano())
}
