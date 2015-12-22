package crocksdb

// #cgo LDFLAGS: -L . -lbridge -L /usr/local/lib -lrocksdb
// #cgo CFLAGS: -I /usr/local/include
// #include "bridge.h"
import "C"
import (
	"time"
	"unsafe"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/store/localstore/engine"
)

var (
	_ engine.DB    = (*db)(nil)
	_ engine.Batch = (*batch)(nil)
)

func cPointer(b []byte) *C.char {
	return (*C.char)(unsafe.Pointer(&b[0]))
}

func cUint32(i int) C.uint32_t {
	return (C.uint32_t)(i)
}

func appendUint32AsByte(b []byte, v uint32) []byte {
	b = append(b, byte(v))
	b = append(b, byte(v>>8))
	b = append(b, byte(v>>16))
	b = append(b, byte(v>>24))
	return b
}

func appendBuf(b []byte, node []byte) []byte {
	ret := appendUint32AsByte(b, uint32(len(node)))
	ret = append(ret, node...)
	return ret
}

func ptrToStr(ptr *C.char, sz C.uint32_t) string {
	ret := C.GoStringN(ptr, (C.int)(sz))
	return ret
}

func ptrToStrAndFree(ptr *C.char, sz C.uint32_t) string {
	ret := C.GoStringN(ptr, (C.int)(sz))
	C.free(unsafe.Pointer(ptr))
	return ret
}

type db struct {
}

type batch struct {
	m map[string][]byte
}

func (b *batch) Put(k, v []byte) {
	b.m[string(k)] = v
}

func (b *batch) Delete(k []byte) {
	b.m[string(k)] = nil
}

func (b *batch) Len() int {
	return len(b.m)
}

func (d *db) open(p string) error {
	path := []byte(p)
	var err *C.char
	C.init(cPointer(path), cUint32(len(path)), (**C.char)(unsafe.Pointer(&err)))
	return nil
}

func (d *db) Get(k []byte) ([]byte, error) {
	var retVal *C.char
	var retSz C.uint32_t
	C.get(cPointer(k), cUint32(len(k)),
		(**C.char)(unsafe.Pointer(&retVal)), (*C.uint32_t)(unsafe.Pointer(&retSz)))
	if retVal != nil {
		s := ptrToStrAndFree(retVal, retSz)
		return []byte(s), nil
	}
	return nil, errors.Trace(engine.ErrNotFound)
}

func (d *db) Seek(startKey []byte) ([]byte, []byte, error) {
	var retVal, retKey *C.char
	var retKeySz, retValSz C.uint32_t
	C.seek(cPointer(startKey), cUint32(len(startKey)),
		(**C.char)(unsafe.Pointer(&retKey)),
		(*C.uint32_t)(unsafe.Pointer(&retKeySz)),
		(**C.char)(unsafe.Pointer(&retVal)),
		(*C.uint32_t)(unsafe.Pointer(&retValSz)),
	)
	if retKey != nil && retVal != nil {
		key := []byte(ptrToStr(retKey, retKeySz))
		val := []byte(ptrToStr(retVal, retValSz))
		return key, val, nil
	}
	return nil, nil, errors.Trace(engine.ErrNotFound)
}

func (d *db) MultiSeek(keys [][]byte) []*engine.MSeekResult {
	var retVal, retKey *C.char
	var retKeySz, retValSz C.uint32_t

	keyBuf := make([]byte, 0, 1024)
	for _, v := range keys {
		keyBuf = appendBuf(keyBuf, []byte(v))
	}

	ct := time.Now()
	C.multi_seek(
		cPointer(keyBuf), cUint32(len(keyBuf)),
		(**C.char)(unsafe.Pointer(&retKey)),
		(*C.uint32_t)(unsafe.Pointer(&retKeySz)),
		(**C.char)(unsafe.Pointer(&retVal)),
		(*C.uint32_t)(unsafe.Pointer(&retValSz)),
	)
	log.Info(time.Since(ct))

	s := ptrToStr(retKey, retKeySz)
	log.Info(s)
	s = ptrToStr(retVal, retValSz)
	log.Info(s)
	return nil
}

func (d *db) Commit(b engine.Batch) error {
	keyBuf := make([]byte, 0, 1024)
	valBuf := make([]byte, 0, 1024)
	for k, v := range b.(*batch).m {
		keyBuf = appendBuf(keyBuf, []byte(k))
		if v == nil {
			// delete mark
			valBuf = appendBuf(valBuf, []byte{0})
		} else {
			valBuf = appendBuf(valBuf, v)
		}
	}
	ct := time.Now()
	C.multi_put(
		cPointer(keyBuf), cUint32(len(keyBuf)),
		cPointer(valBuf), cUint32(len(valBuf)),
	)
	log.Info(time.Since(ct))
	return nil
}

func (d *db) Close() error {
	return nil
}

func (d *db) NewBatch() engine.Batch {
	return &batch{
		m: make(map[string][]byte),
	}
}
