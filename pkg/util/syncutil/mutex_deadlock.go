// Copyright 2023 PingCAP, Inc.
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

//go:build deadlock

package syncutil

import (
	"time"

	deadlock "github.com/sasha-s/go-deadlock"
)

// EnableDeadlock is a flag to enable deadlock detection.
const EnableDeadlock = true

func init() {
	deadlock.Opts.DeadlockTimeout = 20 * time.Second
}

// A Mutex is a mutual exclusion lock.
type Mutex struct {
	deadlock.Mutex
}

// An RWMutex is a reader/writer mutual exclusion lock.
type RWMutex struct {
	deadlock.RWMutex
}
