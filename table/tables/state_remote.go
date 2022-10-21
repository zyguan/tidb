// Copyright 2021 PingCAP, Inc.
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

package tables

import (
	"context"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/oracle"
)

var (
	cacheWriteWaitInvalidate = metrics.TableCacheWriteWaitDurationHistogram.WithLabelValues("invalidate")
	cacheWriteWaitLease      = metrics.TableCacheWriteWaitDurationHistogram.WithLabelValues("lease")
)

// CachedTableLockType define the lock type for cached table
type CachedTableLockType int

const (
	// CachedTableLockNone means there is no lock.
	CachedTableLockNone CachedTableLockType = iota
	// CachedTableLockRead is the READ lock type.
	CachedTableLockRead
	// CachedTableLockIntend is the write INTEND, it exists when the changing READ to WRITE, and the READ lock lease is not expired..
	CachedTableLockIntend
	// CachedTableLockWrite is the WRITE lock type.
	CachedTableLockWrite
)

func (l CachedTableLockType) String() string {
	switch l {
	case CachedTableLockNone:
		return "NONE"
	case CachedTableLockRead:
		return "READ"
	case CachedTableLockIntend:
		return "INTEND"
	case CachedTableLockWrite:
		return "WRITE"
	}
	panic("invalid CachedTableLockType value")
}

// StateRemote is the interface to control the remote state of the cached table's lock meta information.
// IMPORTANT: It's not thread-safe, the caller should be aware of that!
type StateRemote interface {
	// Load obtain the corresponding lock type and lease value according to the tableID
	Load(ctx context.Context, tid int64) (CachedTableLockType, uint64, error)

	// LockForRead try to add a read lock to the table with the specified tableID.
	// If this operation succeed, according to the protocol, the TiKV data will not be
	// modified until the lease expire. It's safe for the caller to load the table data,
	// cache and use the data.
	LockForRead(ctx context.Context, tid int64, lease uint64) (bool, error)

	// LockForWrite try to add a write lock to the table with the specified tableID
	LockForWrite(ctx context.Context, tid int64, leaseDuration time.Duration) (uint64, error)

	// RenewReadLease attempt to renew the read lock lease on the table with the specified tableID
	RenewReadLease(ctx context.Context, tid int64, oldLocalLease, newValue uint64) (uint64, error)

	// RenewWriteLease attempt to renew the write lock lease on the table with the specified tableID
	RenewWriteLease(ctx context.Context, tid int64, newTs uint64) (bool, error)
}

type sqlExec interface {
	ExecuteInternal(context.Context, string, ...interface{}) (sqlexec.RecordSet, error)
}

type cacheService interface {
	LocalAddr() string
	Invalidate(ctx context.Context, addr string, tid int64, minReadLease uint64) (uint64, error)
}

type cacheDataStatus struct {
	maxReadTS uint64
	readLease uint64

	waitTime time.Duration
}

type stateRemoteHandle struct {
	exec  sqlExec
	cache cacheService

	// local state, this could be staled.
	// Since stateRemoteHandle is used in single thread, it's safe for all operations
	// to check the local state first to avoid unnecessary remote TiKV access.
	lockType     CachedTableLockType
	lease        uint64
	oldReadLease uint64
}

// NewStateRemote creates a StateRemote object.
func NewStateRemote(exec sqlExec, cache cacheService) *stateRemoteHandle {
	return &stateRemoteHandle{
		exec:  exec,
		cache: cache,
	}
}

var _ StateRemote = &stateRemoteHandle{}

func (h *stateRemoteHandle) Load(ctx context.Context, tid int64) (CachedTableLockType, uint64, error) {
	lockType, lease, _, err := h.loadRow(ctx, tid, false)
	return lockType, lease, err
}

func (h *stateRemoteHandle) LockForRead(ctx context.Context, tid int64, newLease uint64) ( /*succ*/ bool, error) {
	succ := false
	if h.lease >= newLease {
		// There is a write lock or intention, don't lock for read.
		switch h.lockType {
		case CachedTableLockIntend, CachedTableLockWrite:
			return false, nil
		}
	}

	err := h.runInTxn(ctx, false, func(ctx context.Context, now uint64) error {
		lockType, lease, _, err := h.loadRow(ctx, tid, false)
		if err != nil {
			return errors.Trace(err)
		}
		// The old lock is outdated, clear orphan lock.
		if now > lease {
			if newLease > now { // Note the check, don't decrease the lease value!
				succ = true
				if err := h.updateRow(ctx, tid, "READ", newLease); err != nil {
					return errors.Trace(err)
				}
			}
			return nil
		}

		switch lockType {
		case CachedTableLockNone:
		case CachedTableLockRead:
		case CachedTableLockWrite, CachedTableLockIntend:
			return nil
		}
		succ = true
		if newLease > lease { // Note the check, don't decrease lease value!
			if err := h.updateRow(ctx, tid, "READ", newLease); err != nil {
				return errors.Trace(err)
			}
		}

		return nil
	})
	return succ, err
}

// LockForWrite try to add a write lock to the table with the specified tableID, return the write lock lease.
func (h *stateRemoteHandle) LockForWrite(ctx context.Context, tid int64, leaseDuration time.Duration) (uint64, error) {
	var (
		ret    uint64
		status = cacheDataStatus{maxReadTS: math.MaxUint64}
	)

	if h.lockType == CachedTableLockWrite {
		safe := oracle.GoTimeToTS(time.Now().Add(leaseDuration / 2))
		if h.lease > safe {
			// It means the remote has already been write locked and the lock will be valid for a while.
			// So we can return directly.
			return h.lease, nil
		}
	}

	for {
		wait, lease, err := h.lockForWriteOnce(ctx, tid, leaseDuration, status)
		if err != nil {
			return 0, err
		}
		if wait == nil {
			ret = lease
			break
		}
		status = <-wait
		if status.maxReadTS == math.MaxUint64 {
			cacheWriteWaitLease.Observe(status.waitTime.Seconds())
		} else {
			cacheWriteWaitInvalidate.Observe(status.waitTime.Seconds())
		}
	}
	return ret, nil
}

func (h *stateRemoteHandle) lockForWriteOnce(ctx context.Context, tid int64, leaseDuration time.Duration, status cacheDataStatus) (wait chan cacheDataStatus, ts uint64, err error) {
	var (
		_updateLocal  bool
		_lockType     CachedTableLockType
		_lease        uint64
		_oldReadLease uint64

		leaseWaitDuration time.Duration
	)

	err = h.runInTxn(ctx, true, func(ctx context.Context, now uint64) error {
		lockType, lease, oldReadLease, err := h.loadRow(ctx, tid, true)
		if err != nil {
			return errors.Trace(err)
		}
		ts = leaseFromTS(now, leaseDuration)
		// The lease is outdated, so lock is invalid, clear orphan lock of any kind.
		if now > lease {
			if err := h.updateRow(ctx, tid, "WRITE", ts); err != nil {
				return errors.Trace(err)
			}
			return nil
		}

		// The lease is valid.
		switch lockType {
		case CachedTableLockNone:
			if err = h.updateRow(ctx, tid, "WRITE", ts); err != nil {
				return errors.Trace(err)
			}
			{
				_updateLocal = true
				_lockType = CachedTableLockWrite
				_lease = ts
			}
		case CachedTableLockRead:
			newLease := ts
			if newLease < lease { // Never, never decrease lease
				newLease = lease
			}
			// Change from READ to INTEND
			if _, err = h.execSQL(ctx,
				"update mysql.table_cache_meta set lock_type='INTEND', oldReadLease=%?, lease=%? where tid=%?",
				lease,
				newLease,
				tid); err != nil {
				return errors.Trace(err)
			}

			// Wait for lease to expire, and then retry.
			leaseWaitDuration = waitForLeaseExpire(lease, now)
			{
				_updateLocal = true
				_lockType = CachedTableLockIntend
				_oldReadLease = lease
				_lease = ts
			}
		case CachedTableLockIntend:
			// 1. `now` exceed `oldReadLease` means wait for READ lock lease is done, it's safe to write here.
			// 2. `now` exceed `maxReadTS` within the same lease means all cache data has been invalidated and no more
			//    read from cache after `maxReadTS`, it's also safe to write here.
			if now > oldReadLease || (oldReadLease == status.readLease && now > status.maxReadTS) {
				if err = h.updateRow(ctx, tid, "WRITE", ts); err != nil {
					return errors.Trace(err)
				}
				{
					_updateLocal = true
					_lockType = CachedTableLockWrite
					_lease = ts
				}
				return nil
			}
			// Otherwise, the WRITE should wait for the READ lease expire.
			// And then retry changing the lock to WRITE
			leaseWaitDuration = waitForLeaseExpire(oldReadLease, now)
		case CachedTableLockWrite:
			if ts > lease { // Note the check, don't decrease lease value!
				if err = h.updateRow(ctx, tid, "WRITE", ts); err != nil {
					return errors.Trace(err)
				}
				{
					_updateLocal = true
					_lockType = CachedTableLockWrite
					_lease = ts
				}
			}
		}
		return nil
	})

	if err == nil && _updateLocal {
		h.lockType = _lockType
		h.lease = _lease
		h.oldReadLease = _oldReadLease
		if _lockType == CachedTableLockIntend {
			// leaseWaitDuration >= 1ms when the lock is INTEND because it's equals to waitForLeaseExpire(lease, now)
			// where lease >= now (otherwise we acquire WRITE lock directly since the lease is outdated). Thus MaxUint64
			// must be written to the wait channel after leaseWaitDuration.
			wait = make(chan cacheDataStatus, 2)
			go h.invalidateCache(ctx, tid, _oldReadLease, leaseWaitDuration, wait)
		}
	}
	if leaseWaitDuration > 0 {
		if wait == nil {
			wait = make(chan cacheDataStatus, 1)
		}
		go func() {
			time.Sleep(leaseWaitDuration)
			wait <- cacheDataStatus{maxReadTS: math.MaxUint64, waitTime: leaseWaitDuration}
		}()
	}

	return
}

func (h *stateRemoteHandle) invalidateCache(ctx context.Context, tid int64, oldReadLease uint64, leaseWaitDuration time.Duration, out chan cacheDataStatus) {
	ctx, cancel := context.WithTimeout(ctx, leaseWaitDuration)
	defer cancel()
	startTime := time.Now()
	addrs, err := h.listServers(ctx, tid)
	if err != nil {
		return // failed to list servers
	}

	// send invalidate cache requests to all servers
	var (
		wg        sync.WaitGroup
		lock      sync.Mutex
		maxReadTS uint64
	)
	for i, addr := range addrs {
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			ts, err := h.cache.Invalidate(ctx, addr, tid, oldReadLease)
			lock.Lock()
			if err != nil {
				if maxReadTS != math.MaxUint64 {
					maxReadTS = math.MaxUint64
					cancel()
				}
			} else if ts > maxReadTS {
				maxReadTS = ts
			}
			lock.Unlock()
		}(i, addr)
	}
	wg.Wait()

	if maxReadTS >= oldReadLease {
		return // fallback to waiting read lease
	}

	// calculate wait duration for maxReadTS
	var waitDuration time.Duration
	err = h.runInTxn(ctx, false, func(ctx context.Context, now uint64) error {
		waitDuration = waitForLeaseExpire(maxReadTS, now)
		return nil
	})
	if err != nil {
		return // failed to get wait duration
	}

	// emit cache data status after maxReadTS
	if waitDuration > 0 {
		time.Sleep(waitDuration)
	}
	out <- cacheDataStatus{maxReadTS: maxReadTS, readLease: oldReadLease, waitTime: time.Since(startTime)}
}

func waitForLeaseExpire(oldReadLease, now uint64) time.Duration {
	if oldReadLease >= now {
		t1 := oracle.GetTimeFromTS(oldReadLease)
		t2 := oracle.GetTimeFromTS(now)
		if t1.After(t2) {
			waitDuration := t1.Sub(t2)
			return waitDuration
		}
		return time.Microsecond
	}
	return 0
}

// RenewReadLease renew the read lock lease.
// Return the current lease value on success, and return 0 on fail.
func (h *stateRemoteHandle) RenewReadLease(ctx context.Context, tid int64, oldLocalLease, newValue uint64) (uint64, error) {
	var newLease uint64
	err := h.runInTxn(ctx, false, func(ctx context.Context, now uint64) error {
		lockType, remoteLease, _, err := h.loadRow(ctx, tid, false)
		if err != nil {
			return errors.Trace(err)
		}

		if now >= remoteLease {
			// read lock had already expired, fail to renew
			return nil
		}
		if lockType != CachedTableLockRead {
			// Not read lock, fail to renew
			return nil
		}

		// It means that the lease had already been changed by some other TiDB instances.
		if oldLocalLease != remoteLease {
			// 1. Data in [cacheDataTS -------- oldLocalLease) time range is also immutable.
			// 2. Data in [              now ------------------- remoteLease) time range is immutable.
			//
			// If now < oldLocalLease, it means data in all the time range is immutable,
			// so the old cache data is still available.
			if now < oldLocalLease {
				newLease = remoteLease
			}
			// Otherwise, there might be write operation during the oldLocalLease and the new remoteLease
			// Make renew lease operation fail.
			return nil
		}

		if newValue > remoteLease { // lease should never decrease!
			err = h.updateRow(ctx, tid, "READ", newValue)
			if err != nil {
				return errors.Trace(err)
			}
			newLease = newValue
		} else {
			newLease = remoteLease
		}
		return nil
	})

	return newLease, err
}

func (h *stateRemoteHandle) RenewWriteLease(ctx context.Context, tid int64, newLease uint64) (bool, error) {
	var succ bool
	var (
		_lockType CachedTableLockType
		_lease    uint64
	)
	err := h.runInTxn(ctx, true, func(ctx context.Context, now uint64) error {
		lockType, oldLease, _, err := h.loadRow(ctx, tid, true)
		if err != nil {
			return errors.Trace(err)
		}
		if now >= oldLease {
			// write lock had already expired, fail to renew
			return nil
		}
		if lockType != CachedTableLockWrite {
			// Not write lock, fail to renew
			return nil
		}

		if newLease > oldLease { // lease should never decrease!
			err = h.updateRow(ctx, tid, "WRITE", newLease)
			if err != nil {
				return errors.Trace(err)
			}
		}
		succ = true
		_lockType = CachedTableLockWrite
		_lease = newLease
		return nil
	})

	if succ {
		h.lockType = _lockType
		h.lease = _lease
	}
	return succ, err
}

func (h *stateRemoteHandle) beginTxn(ctx context.Context, pessimistic bool) error {
	var err error
	if pessimistic {
		_, err = h.execSQL(ctx, "begin pessimistic")
	} else {
		_, err = h.execSQL(ctx, "begin optimistic")
	}
	return err
}

func (h *stateRemoteHandle) commitTxn(ctx context.Context) error {
	_, err := h.execSQL(ctx, "commit")
	return err
}

func (h *stateRemoteHandle) rollbackTxn(ctx context.Context) error {
	_, err := h.execSQL(ctx, "rollback")
	return err
}

func (h *stateRemoteHandle) runInTxn(ctx context.Context, pessimistic bool, fn func(ctx context.Context, txnTS uint64) error) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMeta)
	err := h.beginTxn(ctx, pessimistic)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = h.execSQL(ctx, "set @@session.tidb_retry_limit = 0")
	if err != nil {
		return errors.Trace(err)
	}

	rows, err := h.execSQL(ctx, "select @@tidb_current_ts")
	if err != nil {
		return errors.Trace(err)
	}
	resultStr := rows[0].GetString(0)
	txnTS, err := strconv.ParseUint(resultStr, 10, 64)
	if err != nil {
		return errors.Trace(err)
	}

	err = fn(ctx, txnTS)
	if err != nil {
		terror.Log(h.rollbackTxn(ctx))
		return errors.Trace(err)
	}

	return h.commitTxn(ctx)
}

func (h *stateRemoteHandle) loadRow(ctx context.Context, tid int64, forUpdate bool) (CachedTableLockType, uint64, uint64, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMeta)
	var chunkRows []chunk.Row
	var err error
	if forUpdate {
		chunkRows, err = h.execSQL(ctx, "select lock_type, lease, oldReadLease from mysql.table_cache_meta where tid = %? for update", tid)
	} else {
		chunkRows, err = h.execSQL(ctx, "select lock_type, lease, oldReadLease from mysql.table_cache_meta where tid = %?", tid)
	}
	if err != nil {
		return 0, 0, 0, errors.Trace(err)
	}
	if len(chunkRows) != 1 {
		return 0, 0, 0, errors.Errorf("table_cache_meta tid not exist %d", tid)
	}
	col1 := chunkRows[0].GetEnum(0)
	// Note, the MySQL enum value start from 1 rather than 0
	lockType := CachedTableLockType(col1.Value - 1)
	lease := chunkRows[0].GetUint64(1)
	oldReadLease := chunkRows[0].GetUint64(2)

	// Also store a local copy after loadRow()
	h.lockType = lockType
	h.lease = lease
	h.oldReadLease = oldReadLease

	return lockType, lease, oldReadLease, nil
}

func (h *stateRemoteHandle) updateRow(ctx context.Context, tid int64, lockType string, lease uint64) error {
	_, err := h.execSQL(ctx, "update mysql.table_cache_meta set lock_type = %?, lease = %? where tid = %?", lockType, lease, tid)
	if err != nil {
		return err
	}
	if lockType == "READ" {
		_, err = h.execSQL(ctx, "insert into mysql.table_cache_data (table_id, server, lease) values (%?, %?, %?) on duplicate key update lease = values(lease)", tid, h.cache.LocalAddr(), lease)
	} else if lockType == "WRITE" {
		_, err = h.execSQL(ctx, "delete from mysql.table_cache_data where table_id = %?", tid)
	}
	return err
}

func (h *stateRemoteHandle) listServers(ctx context.Context, tid int64) ([]string, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMeta)
	chunkRows, err := h.execSQL(ctx, "select server from mysql.table_cache_data where table_id = %?", tid)
	if err != nil {
		return nil, errors.Trace(err)
	}
	servers := make([]string, len(chunkRows))
	for i, row := range chunkRows {
		servers[i] = row.GetString(0)
	}
	return servers, nil
}

func (h *stateRemoteHandle) execSQL(ctx context.Context, sql string, args ...interface{}) ([]chunk.Row, error) {
	rs, err := h.exec.ExecuteInternal(ctx, sql, args...)
	if rs != nil {
		//nolint: errcheck
		defer rs.Close()
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	if rs != nil {
		return sqlexec.DrainRecordSet(ctx, rs, 1)
	}
	return nil, nil
}
