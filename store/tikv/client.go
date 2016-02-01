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

// Package tikv provides tcp connect kvserver.
package tikv

import (
	"net"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/store/tikv/kvrpc"
	pb "github.com/pingcap/tidb/store/tikv/kvrpc/proto"
)

// Client connect kvserver to send Request by TCP.
type Client struct {
	dst   net.TCPAddr
	msgID uint64
	mu    sync.Mutex
}

// NewClient new client, e.g.: NewClient("192.168.1.2:61234").
func NewClient(srvHost string) *Client {
	addr, err := net.ResolveTCPAddr("tcp4", srvHost)
	if err != nil {
		log.Error("Parse server host error srv[%s]", srvHost)
		return nil
	}
	return &Client{
		dst:   *addr,
		msgID: 0,
	}
}

// Send send a Request and receive Response.
func (c *Client) Send(req *pb.Request) (*pb.Response, error) {
	conn, err := net.DialTCP("tcp", nil, &c.dst)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = conn.SetNoDelay(true)
	if err != nil {
		log.Warn("Set nodelay failed.")
	}
	defer conn.Close()
	c.mu.Lock()
	c.msgID++
	c.mu.Unlock()
	err = kvrpc.EncodeMessage(conn, c.msgID, req)
	log.Debugf("Send request msgID[%d] type[%s]", c.msgID, req.GetType())
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp := new(pb.Response)
	msgID, err := kvrpc.DecodeMessage(conn, resp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Debugf("Receive response msgID[%d]", msgID)

	return resp, nil
}

// Close close client
func (c *Client) Close() error {
	return nil
}
