package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv"
	"go.uber.org/zap"

	_ "github.com/go-sql-driver/mysql"
)

var options struct {
	User string
	Pass string
	Host string
	Port int

	Concurrency int
	TableFilter string
	Output      string
}

func init() {
	flag.StringVar(&options.User, "u", "root", "username")
	flag.StringVar(&options.Pass, "p", "", "password")
	flag.StringVar(&options.Host, "h", "127.0.0.1", "hostname")
	flag.IntVar(&options.Port, "P", 4000, "port")

	flag.IntVar(&options.Concurrency, "c", 1, "concurrency")
	flag.StringVar(&options.TableFilter, "f", "test.*", "table filter")
	flag.StringVar(&options.Output, "o", "", "output file")

}

func main() {
	flag.Parse()
	mustReplaceLogOut()
	mustInitGlobal()

	tbls, err := listTables(global.DB, options.TableFilter)
	perror(err)

	c := options.Concurrency
	if c <= 0 || c > len(tbls) {
		c = len(tbls)
	}
	var (
		wg   sync.WaitGroup
		in   = make(chan TableInfo)
		out  = make(chan *MVCCResponse)
		done = make(chan struct{})
		f    = os.Stdout
		enc  = json.NewEncoder(f)
	)
	if len(options.Output) > 0 {
		f, err = os.OpenFile(options.Output, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		perror(err)
		defer f.Close()
	}
	go func() {
		for _, tbl := range tbls {
			in <- tbl
		}
		close(in)
	}()
	for i := 0; i < c; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tbl := range in {
				logutil.BgLogger().Info("dumping "+tbl.Name, zap.Int64("id", tbl.ID))
				if err := dumpTableMvcc(out, global.KV, tbl); err != nil {
					logutil.BgLogger().Warn("failed to dump "+tbl.Name, zap.Error(err))
				}
			}
		}()
	}
	go func() {
		defer close(done)
		for resp := range out {
			var info struct {
				Key   string            `json:"key"`
				Table string            `json:"table"`
				Error string            `json:"error,omitempty"`
				MVCC  *kvrpcpb.MvccInfo `json:"mvcc,omitempty"`
			}
			info.Key = resp.Key.String()
			info.Table = resp.Table.Name
			if resp.Err != nil {
				info.Error = resp.Err.Error()
			} else if len(resp.Resp.Error) > 0 {
				info.Error = resp.Resp.Error
			} else if resp.Resp.RegionError != nil {
				info.Error = resp.Resp.RegionError.String()
			} else {
				info.MVCC = resp.Resp.Info
			}
			enc.Encode(info)
		}
	}()
	wg.Wait()
	close(out)
	<-done
}

func perror(err error) {
	if err != nil {
		panic(err)
	}
}

var global struct {
	PDs []string
	DB  *sql.DB
	KV  *txnkv.Client
}

func mustInitGlobal() {
	var err error
	global.DB, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/", options.User, options.Pass, options.Host, options.Port))
	perror(err)
	global.PDs, err = listStatusAddresses(global.DB, "pd")
	perror(err)
	global.KV, err = txnkv.NewClient(global.PDs)
	perror(err)
}

func mustReplaceLogOut() {
	logOut, _, err := zap.Open("stderr")
	perror(err)
	logger, props, err := log.InitLoggerWithWriteSyncer(&log.Config{Level: "info", File: log.FileLogConfig{}}, logOut, logOut)
	perror(err)
	log.ReplaceGlobals(logger, props)
}

func listStatusAddresses(db *sql.DB, comp string) ([]string, error) {
	rows, err := db.Query("select status_address from information_schema.cluster_info where type = ?", comp)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var addrs []string
	for rows.Next() {
		var addr string
		if err = rows.Scan(&addr); err != nil {
			return nil, err
		}
		addrs = append(addrs, addr)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return addrs, nil
}

type TableInfo struct {
	ID   int64
	Name string
}

func listTables(db *sql.DB, re string) ([]TableInfo, error) {
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

func getMvccByKey(kv *txnkv.Client, k kv.Key) (*kvrpcpb.MvccGetByKeyResponse, error) {
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

type MVCCResponse struct {
	Table *TableInfo
	Key   kv.Key
	Err   error
	Resp  *kvrpcpb.MvccGetByKeyResponse
}

func dumpTableMvcc(dst chan<- *MVCCResponse, kv *txnkv.Client, tbl TableInfo) error {
	lower, upper := tablecodec.GenTablePrefix(tbl.ID), tablecodec.GenTablePrefix(tbl.ID+1)
	it, err := kv.GetSnapshot(math.MaxUint64).Iter(lower, upper)
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Next() == nil {
		resp := &MVCCResponse{Table: &tbl, Key: it.Key()}
		if len(resp.Key) == 0 {
			continue
		}
		for i := 0; i < 3; i++ {
			resp.Resp, resp.Err = getMvccByKey(kv, resp.Key)
			if resp.Err != nil || resp.Resp.RegionError == nil {
				break
			}
		}
		dst <- resp
	}
	return nil
}
