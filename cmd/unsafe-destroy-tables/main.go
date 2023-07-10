package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

func main() {
	var (
		sec config.Security
		pds []string
	)
	split := func(arg *[]string) func(s string) error {
		return func(s string) error {
			if len(s) > 0 {
				*arg = strings.Split(s, ",")
			}
			return nil
		}
	}
	flag.StringVar(&sec.ClusterSSLCA, "ssl-ca", "", "path to ssl ca file")
	flag.StringVar(&sec.ClusterSSLCert, "ssl-cert", "", "path to ssl cert file")
	flag.StringVar(&sec.ClusterSSLKey, "ssl-key", "", "path to ssl key file")
	flag.Func("verify-cn", "verify certificate common name", split(&sec.ClusterVerifyCN))
	flag.Func("pd", "pd address", split(&pds))
	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Println("Usage: unsafe-destroy-tables <table_id> [table_id...]")
		return
	}
	tids := make([]int64, len(flag.Args()))
	for i, arg := range flag.Args() {
		tid, err := strconv.ParseInt(arg, 10, 64)
		if err != nil {
			logutil.BgLogger().Fatal("[unsafe-destroy] invalid table id", zap.Int("index", i), zap.String("value", arg))
		}
		tids[i] = tid
	}
	ctx := context.Background()
	cli := mustOpen(ctx, pds, sec)
	defer cli.Close()
	unsafeDestroyTables(context.Background(), cli, tids)
}

type ClientSet struct {
	PD   pd.Client
	KV   *tikv.KVStore
	SPKV *tikv.EtcdSafePointKV
}

func (c *ClientSet) Close() error {
	c.SPKV.Close()
	c.KV.Close()
	c.PD.Close()
	return nil
}

func mustOpen(ctx context.Context, pds []string, sec config.Security) *ClientSet {
	var (
		err        error
		cli        ClientSet
		tlsConfig  *tls.Config
		tlsEnabled = len(sec.ClusterSSLCA) > 0
	)
	if tlsEnabled {
		tlsConfig, err = sec.ToTLSConfig()
		if err != nil {
			logutil.Logger(ctx).Fatal("[unsafe-destroy] got an error while trying to build tls config",
				zap.Error(err))
		}
	}

	if tlsEnabled {
		cli.PD, err = pd.NewClient(pds, pd.SecurityOption{
			CAPath:   sec.ClusterSSLCA,
			CertPath: sec.ClusterSSLCert,
			KeyPath:  sec.ClusterSSLKey,
		})
	} else {
		cli.PD, err = pd.NewClient(pds, pd.SecurityOption{})
	}
	if err != nil {
		logutil.Logger(ctx).Fatal("[unsafe-destroy] got an error while trying to create pd client",
			zap.Error(err))
	}

	if tlsEnabled {
		cli.SPKV, err = tikv.NewEtcdSafePointKV(pds, tlsConfig)
	} else {
		cli.SPKV, err = tikv.NewEtcdSafePointKV(pds, nil)
	}
	if err != nil {
		cli.PD.Close()
		logutil.Logger(ctx).Fatal("[unsafe-destroy] got an error while trying to create safe point kv",
			zap.Error(err))
	}

	rpcClient := tikv.NewRPCClient(tikv.WithSecurity(sec))
	if tlsEnabled {
		cli.KV, err = tikv.NewKVStore(fmt.Sprintf("tikv-%v", cli.PD.GetClusterID(ctx)), cli.PD, cli.SPKV, rpcClient, tikv.WithPDHTTPClient(tlsConfig, pds))
	} else {
		cli.KV, err = tikv.NewKVStore(fmt.Sprintf("tikv-%v", cli.PD.GetClusterID(ctx)), cli.PD, cli.SPKV, rpcClient)
	}
	if err != nil {
		cli.SPKV.Close()
		cli.PD.Close()
		logutil.Logger(ctx).Fatal("[unsafe-destroy] got an error while trying to create tikv store",
			zap.Error(err))
	}

	return &cli
}

func unsafeDestroyTables(ctx context.Context, cli *ClientSet, tids []int64) {
	for _, tid := range tids {
		startKey := tablecodec.EncodeTablePrefix(tid)
		endKey := tablecodec.EncodeTablePrefix(tid + 1)

		logutil.Logger(ctx).Info("[unsafe-destroy] start delete table",
			zap.Int64("tableID", tid),
			zap.Stringer("startKey", startKey),
			zap.Stringer("endKey", endKey))
		err := doUnsafeDestroyRangeRequest(ctx, cli.PD, cli.KV, startKey, endKey)
		if err != nil {
			logutil.Logger(ctx).Error("[unsafe-destroy] unsafe delete range failed",
				zap.Int64("tableID", tid),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey),
				zap.Error(err))
		}
		logutil.Logger(ctx).Info("[unsafe-destroy] finish delete table",
			zap.Int64("tableID", tid),
			zap.Stringer("startKey", startKey),
			zap.Stringer("endKey", endKey))
	}
}

func doUnsafeDestroyRangeRequest(ctx context.Context, pdClient pd.Client, tikvStore *tikv.KVStore, startKey []byte, endKey []byte) error {
	// Get all stores every time deleting a region. So the store list is less probably to be stale.
	stores, err := getStoresForGC(ctx, pdClient)
	if err != nil {
		logutil.Logger(ctx).Error("[unsafe-destroy] delete ranges: got an error while trying to get store list from PD",
			zap.Error(err))
		return err
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdUnsafeDestroyRange, &kvrpcpb.UnsafeDestroyRangeRequest{
		StartKey: startKey,
		EndKey:   endKey,
	}, kvrpcpb.Context{DiskFullOpt: kvrpcpb.DiskFullOpt_AllowedOnAlmostFull})

	var wg sync.WaitGroup
	errChan := make(chan error, len(stores))

	for _, store := range stores {
		address := store.Address
		storeID := store.Id
		wg.Add(1)
		go func() {
			defer wg.Done()

			resp, err1 := tikvStore.GetTiKVClient().SendRequest(ctx, address, req, 5*time.Minute)
			if err1 == nil {
				if resp == nil || resp.Resp == nil {
					err1 = errors.Errorf("unsafe destroy range returns nil response from store %v", storeID)
				} else {
					errStr := (resp.Resp.(*kvrpcpb.UnsafeDestroyRangeResponse)).Error
					if len(errStr) > 0 {
						err1 = errors.Errorf("unsafe destroy range failed on store %v: %s", storeID, errStr)
					}
				}
			}

			errChan <- err1
		}()
	}

	var errs []string
	for range stores {
		err1 := <-errChan
		if err1 != nil {
			errs = append(errs, err1.Error())
		}
	}

	wg.Wait()

	if len(errs) > 0 {
		return errors.Errorf("[unsafe-destroy] destroy range finished with errors: %v", errs)
	}

	return nil
}

func getStoresForGC(ctx context.Context, pdClient pd.Client) ([]*metapb.Store, error) {
	stores, err := pdClient.GetAllStores(ctx)
	if err != nil {
		return nil, err
	}

	upStores := make([]*metapb.Store, 0, len(stores))
	for _, store := range stores {
		needsGCOp, err := needsGCOperationForStore(store)
		if err != nil {
			return nil, err
		}
		if needsGCOp {
			upStores = append(upStores, store)
		}
	}
	return upStores, nil
}

func needsGCOperationForStore(store *metapb.Store) (bool, error) {
	// TombStone means the store has been removed from the cluster and there isn't any peer on the store, so needn't do GC for it.
	// Offline means the store is being removed from the cluster and it becomes tombstone after all peers are removed from it,
	// so we need to do GC for it.
	if store.State == metapb.StoreState_Tombstone {
		return false, nil
	}

	engineLabel := ""
	for _, label := range store.GetLabels() {
		if label.GetKey() == placement.EngineLabelKey {
			engineLabel = label.GetValue()
			break
		}
	}

	switch engineLabel {
	case placement.EngineLabelTiFlash:
		// For a TiFlash node, it uses other approach to delete dropped tables, so it's safe to skip sending
		// UnsafeDestroyRange requests; it has only learner peers and their data must exist in TiKV, so it's safe to
		// skip physical resolve locks for it.
		return false, nil

	case placement.EngineLabelTiFlashCompute:
		logutil.BgLogger().Debug("[unsafe-destroy] will ignore gc tiflash_compute node")
		return false, nil

	case placement.EngineLabelTiKV, "":
		// If no engine label is set, it should be a TiKV node.
		return true, nil

	default:
		return true, errors.Errorf("unsupported store engine \"%v\" with storeID %v, addr %v",
			engineLabel,
			store.GetId(),
			store.GetAddress())
	}
}
