package crocksdb

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/localstore/engine"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	db engine.DB
}

func (s *testSuite) SetUpSuite(c *C) {
	db := &db{}
	db.open("/tmp/tidb_crocks")
	s.db = db
}

func (s *testSuite) TearDownSuite(c *C) {
	s.db.Close()
}

func (s *testSuite) TestPut(c *C) {
	b := s.db.NewBatch()
	for i := 0; i < 10000; i++ {
		k := []byte(fmt.Sprintf("row_%d", i))
		b.Put(k, k)
	}
	s.db.Commit(b)
	var t [][]byte
	for i := 0; i < 1000; i++ {
		t = append(t, []byte(fmt.Sprintf("row_%d", i)))
	}
	s.db.MultiSeek(t)
}
