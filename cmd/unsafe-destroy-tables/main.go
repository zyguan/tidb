package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

func main() {
	pd := flag.String("pd", "127.0.0.1:2379", "pd address")
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
	unsafeDestroyTables(context.Background(), []string{*pd}, tids)
}

func unsafeDestroyTables(ctx context.Context, pds []string, tids []int64) {
	pdClient, err := tikv.NewPDClient(pds)
	if err != nil {
		logutil.Logger(ctx).Fatal("[unsafe-destroy] delete ranges: got an error while trying to create pd client",
			zap.Error(err))
	}
	defer pdClient.Close()
	spkv, err := tikv.NewEtcdSafePointKV(pds, nil)
	if err != nil {
		logutil.Logger(ctx).Fatal("[unsafe-destroy] delete ranges: got an error while trying to create safe point kv",
			zap.Error(err))
	}
	defer spkv.Close()
	tikvStore, err := tikv.NewKVStore(fmt.Sprintf("tikv-%v", pdClient.GetClusterID(ctx)), pdClient, spkv, tikv.NewRPCClient())
	if err != nil {
		logutil.Logger(ctx).Fatal("[unsafe-destroy] delete ranges: got an error while trying to create tikv store",
			zap.Error(err))
	}
	defer tikvStore.Close()

	for _, tid := range tids {
		startKey := tablecodec.EncodeTablePrefix(tid)
		endKey := tablecodec.EncodeTablePrefix(tid + 1)

		logutil.Logger(ctx).Info("[unsafe-destroy] start delete table",
			zap.Int64("tableID", tid),
			zap.Stringer("startKey", startKey),
			zap.Stringer("endKey", endKey))
		err := doUnsafeDestroyRangeRequest(ctx, pdClient, tikvStore, startKey, endKey)
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
