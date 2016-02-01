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
	"net"
	"testing"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/tikv/kvrpc"
	pb "github.com/pingcap/tidb/store/tikv/kvrpc/proto"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testClientSuite{})

type testClientSuite struct {
	l net.Listener
}

const (
	ip         = "127.0.0.1"
	port       = 64321
	remoteIP   = "127.0.0.1"
	remotePort = 61234
)

func (s *testClientSuite) SetUpSuite(c *C) {
	var err error
	s.l, err = net.Listen("tcp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		log.Fatal("Error listening: ", err)
	}
	log.Debug("Start listenning...")
	go func() {
		for {
			conn, err := s.l.Accept()
			if err != nil {
				log.Fatal("Error accepting: ", err)
			}
			go handleRequest(conn)
		}
	}()
}

func (s *testClientSuite) TearDownSuite(c *C) {
	s.l.Close()
	log.Debug("Stop listenning.")
}

// handleRequest receive Request then send empty Response back fill with same Type
func handleRequest(conn net.Conn) {
	defer conn.Close()
	req := new(pb.Request)
	msgID, err := kvrpc.DecodeMessage(conn, req)
	if err != nil {
		log.Error("Error reading: ", err)
		return
	}
	resp := new(pb.Response)
	resp.Type = req.Type
	err = kvrpc.EncodeMessage(conn, msgID, resp)
	if err != nil {
		log.Error("Error writing: ", err)
		return
	}
}

func (s *testClientSuite) TestSendBySelf(c *C) {
	cli := NewClient(fmt.Sprintf("%s:%d", ip, port))
	req := new(pb.Request)
	getType := pb.MessageType_CmdGet
	req.Type = &getType
	getReq := new(pb.CmdGetRequest)
	getReq.Key = []byte("a")
	ver := uint64(0)
	getReq.Version = &ver
	req.CmdGetReq = getReq
	resp, err := cli.Send(req)
	c.Assert(err, IsNil)
	c.Assert(req.GetType(), Equals, resp.GetType())
}
func (s *testClientSuite) TestSend(c *C) {
	cli := NewClient(fmt.Sprintf("%s:%d", remoteIP, remotePort))
	req := new(pb.Request)
	getType := pb.MessageType_CmdGet
	req.Type = &getType
	getReq := new(pb.CmdGetRequest)
	getReq.Key = []byte("a")
	ver := uint64(0)
	getReq.Version = &ver
	req.CmdGetReq = getReq
	for i := 0; i < 10; i++ {
		resp, err := cli.Send(req)
		c.Assert(err, IsNil)
		c.Assert(req.GetType(), Equals, resp.GetType())
	}
}
