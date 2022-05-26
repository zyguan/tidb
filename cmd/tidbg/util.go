package main

import (
	"context"
	"database/sql"
	"io"
	"time"

	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/codec"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func dbListComponentInfo(db *sql.DB, comp string, field string) ([]string, error) {
	rows, err := db.Query("select "+field+" from information_schema.cluster_info where type = ?", comp)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var vals []string
	for rows.Next() {
		var val string
		if err = rows.Scan(&val); err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return vals, nil
}

func dbListInstances(db *sql.DB, comp string) ([]string, error) {
	return dbListComponentInfo(db, comp, "instance")
}

type TableInfo struct {
	ID   int64
	Name string
}

func dbListTables(db *sql.DB, re string) ([]TableInfo, error) {
	rows, err := db.Query("select tidb_table_id, concat(table_schema, '.', table_name) from information_schema.tables where tidb_table_id is not null and concat(table_schema, '.', table_name) regexp ? union select tidb_partition_id, concat(table_schema, '.', table_name, '.', partition_name) from information_schema.partitions where tidb_partition_id is not null and concat(table_schema, '.', table_name, '.', partition_name) regexp ?", re, re)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tbls []TableInfo
	for rows.Next() {
		var tbl TableInfo
		if err = rows.Scan(&tbl.ID, &tbl.Name); err != nil {
			return nil, err
		}
		tbls = append(tbls, tbl)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return tbls, nil
}

func kvGetMvccByKey(kv *tikv.KVStore, k kv.Key) (*kvrpcpb.MvccGetByKeyResponse, error) {
	bo := tikv.NewBackofferWithVars(context.TODO(), 1000, nil)
	loc, err := kv.GetRegionCache().LocateKey(bo, k)
	if err != nil {
		return nil, err
	}
	req := tikvrpc.NewRequest(tikvrpc.CmdMvccGetByKey, &kvrpcpb.MvccGetByKeyRequest{Key: k})
	resp, err := kv.SendReq(bo, req, loc.Region, time.Minute)
	if err != nil {
		return nil, err
	}
	return resp.Resp.(*kvrpcpb.MvccGetByKeyResponse), nil
}

func debugScanMvcc(debug debugpb.DebugClient, req *debugpb.ScanMvccRequest, cb func(resp *debugpb.ScanMvccResponse)) error {
	stream, err := debug.ScanMvcc(context.TODO(), req)
	if err != nil {
		return err
	}
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		cb(resp)
	}
	return nil
}

func toKvMvccKey(k kv.Key) []byte {
	return codec.EncodeBytes([]byte{'z'}, k)
}

func fromKvMvccKey(raw []byte) (kv.Key, error) {
	if len(raw) == 0 || raw[0] != 'z' {
		return raw, nil
	}
	_, k, err := codec.DecodeBytes(raw[1:], nil)
	if err != nil {
		return raw, err
	}
	return k, nil
}
