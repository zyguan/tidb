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

package parse

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/server/internal/handshake"
	"github.com/stretchr/testify/require"
)

func TestAuthSwitchRequest(t *testing.T) {
	// this data is from a MySQL 8.0 client
	data := []byte{
		0x85, 0xa6, 0xff, 0x1, 0x0, 0x0, 0x0, 0x1, 0x21, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x72, 0x6f,
		0x6f, 0x74, 0x0, 0x0, 0x63, 0x61, 0x63, 0x68, 0x69, 0x6e, 0x67, 0x5f, 0x73, 0x68, 0x61,
		0x32, 0x5f, 0x70, 0x61, 0x73, 0x73, 0x77, 0x6f, 0x72, 0x64, 0x0, 0x79, 0x4, 0x5f, 0x70,
		0x69, 0x64, 0x5, 0x37, 0x37, 0x30, 0x38, 0x36, 0x9, 0x5f, 0x70, 0x6c, 0x61, 0x74, 0x66,
		0x6f, 0x72, 0x6d, 0x6, 0x78, 0x38, 0x36, 0x5f, 0x36, 0x34, 0x3, 0x5f, 0x6f, 0x73, 0x5,
		0x4c, 0x69, 0x6e, 0x75, 0x78, 0xc, 0x5f, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x6e,
		0x61, 0x6d, 0x65, 0x8, 0x6c, 0x69, 0x62, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x7, 0x6f, 0x73,
		0x5f, 0x75, 0x73, 0x65, 0x72, 0xa, 0x6e, 0x75, 0x6c, 0x6c, 0x6e, 0x6f, 0x74, 0x6e, 0x69,
		0x6c, 0xf, 0x5f, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69,
		0x6f, 0x6e, 0x6, 0x38, 0x2e, 0x30, 0x2e, 0x32, 0x31, 0xc, 0x70, 0x72, 0x6f, 0x67, 0x72,
		0x61, 0x6d, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x5, 0x6d, 0x79, 0x73, 0x71, 0x6c,
	}

	var resp handshake.Response41
	pos, err := HandshakeResponseHeader(context.Background(), &resp, data)
	require.NoError(t, err)
	err = HandshakeResponseBody(context.Background(), &resp, data, pos)
	require.NoError(t, err)
	require.Equal(t, "caching_sha2_password", resp.AuthPlugin)
}
