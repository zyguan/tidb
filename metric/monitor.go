// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package metric

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/pingcap/tidb/util/printer"
	"github.com/rcrowley/go-metrics"
	"github.com/vrischmann/go-metrics-influxdb"
)

type sinkType byte

var (
	toWhere sinkType
	inxConf influxConf
	lgConf  logConf
)

const (
	logSink = iota + 1
	influxSink
)

type influxConf struct {
	d        time.Duration
	url      string
	database string
	username string
	password string
}

type logConf struct {
	d      time.Duration
	logger *log.Logger
}

type context struct {
	prefix  string
	tag     string
	gitHash string
	buildTS string
}

func newRegistry(sink sinkType) metrics.Registry {
	registry := metrics.NewRegistry()
	if metrics.UseNilMetrics {
		return registry
	}
	switch sink {
	case influxSink:
		go influxdb.InfluxDB(
			registry,
			inxConf.d,
			inxConf.url,
			inxConf.database,
			inxConf.username,
			inxConf.password,
		)
	case logSink:
		go metrics.Log(registry, lgConf.d, lgConf.logger)
	default:
		break
	}
	return registry
}

// KVMonitor monitor kv store time cost.
type KVMonitor struct {
	registry metrics.Registry
	ctx      context
	// Below is custom metrics.
	callCount metrics.Counter
	getTimer  metrics.Timer
	setTimer  metrics.Timer
	seekTimer metrics.Timer
	incTimer  metrics.Timer
}

// OnGetFinish be invoked after kv Get() finish.
func (m *KVMonitor) OnGetFinish(start time.Time) {
	m.getTimer.UpdateSince(start)
	m.callCount.Inc(1)
}

// OnSetFinish be invoked after kv Set() finish.
func (m *KVMonitor) OnSetFinish(start time.Time) {
	m.setTimer.UpdateSince(start)
	m.callCount.Inc(1)
}

// OnSeekFinish be invoked after kv Seek() finish.
func (m *KVMonitor) OnSeekFinish(start time.Time) {
	m.seekTimer.UpdateSince(start)
	m.callCount.Inc(1)
}

// OnIncFinish be invoked after kv Inc() finish.
func (m *KVMonitor) OnIncFinish(start time.Time) {
	m.incTimer.UpdateSince(start)
	m.callCount.Inc(1)
}

// NewKVMonitor return new KV monitor.
func NewKVMonitor(prefix string) *KVMonitor {
	registry := newRegistry(toWhere)
	ctx := context{
		prefix:  prefix,
		tag:     "kv",
		gitHash: printer.TiDBGitHash,
		buildTS: printer.TiDBBuildTS,
	}
	return &KVMonitor{
		registry:  registry,
		ctx:       ctx,
		callCount: metrics.NewRegisteredCounter(metricName(ctx, "call"), registry),
		getTimer:  metrics.NewRegisteredTimer(metricName(ctx, "get"), registry),
		setTimer:  metrics.NewRegisteredTimer(metricName(ctx, "set"), registry),
		seekTimer: metrics.NewRegisteredTimer(metricName(ctx, "seek"), registry),
		incTimer:  metrics.NewRegisteredTimer(metricName(ctx, "inc"), registry),
	}
}

func metricName(ctx context, name string) string {
	return fmt.Sprintf("%s.%s.%s", ctx.prefix, ctx.tag, name)
}

func init() {
	toWhere = influxSink
	// For influx sink.
	// TODO: read from conf.
	inxConf.d = 1 * time.Second
	inxConf.url = "http://localhost:8086"
	inxConf.database = "tidb"
	inxConf.username = "myuser"
	inxConf.password = "mypassword"
	// For log sink.
	lgConf.d = 1 * time.Second
	lgConf.logger = log.New(os.Stderr, "metrics: ", log.Lmicroseconds)
}
