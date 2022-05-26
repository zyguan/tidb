package main

import (
	"encoding/hex"

	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func newMvccCmd(out Output, flags *ClientFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mvcc",
		Short: "MVCC utilities",
	}
	cmd.AddCommand(newMvccGetCmd(out, flags))
	cmd.AddCommand(newMvccScanCmd(out, flags))
	cmd.AddCommand(newMvccScanTableCmd(out, flags))
	return cmd
}

func newMvccGetCmd(out Output, flags *ClientFlags) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get",
		Short: "Get MVCC by keys",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var cli ClientSet
			kv, err := cli.GetKV(*flags)
			if err != nil {
				return err
			}
			var item struct {
				Key  string            `json:"key"`
				Err  string            `json:"err,omitempty"`
				Info *kvrpcpb.MvccInfo `json:"mvcc,omitempty"`
			}
			for _, s := range args {
				k, err := hex.DecodeString(s)
				if err != nil {
					logutil.BgLogger().Warn("invalid hex key", zap.String("key", s), zap.Error(err))
					continue
				}
				item.Key = s
				resp, err := kvGetMvccByKey(kv, k)
				if err != nil {
					item.Err = err.Error()
				} else if len(resp.Error) > 0 {
					item.Err = resp.Error
				} else if resp.RegionError != nil {
					item.Err = resp.RegionError.String()
				} else {
					item.Info = resp.Info
				}
				out.Dump(item)
			}
			return nil
		},
	}
	return cmd
}

func newMvccScanCmd(out Output, flags *ClientFlags) *cobra.Command {
	var req debugpb.ScanMvccRequest
	cmd := &cobra.Command{
		Use:   "scan",
		Short: "Scan MVCC by keys",
		RunE: func(cmd *cobra.Command, args []string) error {
			var cli ClientSet
			debug, err := cli.GetDebug(*flags)
			if err != nil {
				return err
			}
			if len(req.FromKey) > 0 && req.FromKey[0] != 'z' {
				req.FromKey = toKvMvccKey(req.FromKey)
			}
			if len(req.ToKey) > 0 && req.ToKey[0] != 'z' {
				req.ToKey = toKvMvccKey(req.ToKey)
			}
			debugScanMvcc(debug, &req, func(resp *debugpb.ScanMvccResponse) {
				var item struct {
					Key  string            `json:"key"`
					Mvcc *kvrpcpb.MvccInfo `json:"mvcc,omitempty"`
				}
				key, err := fromKvMvccKey(resp.Key)
				if err != nil {
					logutil.BgLogger().Warn("invalid kv mvcc key", zap.Error(err))
				}
				item.Key = hex.EncodeToString(key)
				item.Mvcc = resp.Info
				out.Dump(item)
			})
			return nil
		},
	}
	cmd.Flags().BytesHexVarP(&req.FromKey, "from", "f", []byte{}, "Scan from key.")
	cmd.Flags().BytesHexVarP(&req.ToKey, "to", "t", []byte{}, "Scan to key.")
	cmd.Flags().Uint64Var(&req.Limit, "limit", 0, "Scan limit.")
	return cmd
}

func newMvccScanTableCmd(out Output, flags *ClientFlags) *cobra.Command {
	var filter string
	cmd := &cobra.Command{
		Use:   "scan-tables",
		Short: "Scan MVCC by tables",
		RunE: func(cmd *cobra.Command, args []string) error {
			var cli ClientSet
			db, err := cli.GetDB(*flags)
			if err != nil {
				return err
			}
			tbls, err := dbListTables(db, filter)
			if err != nil {
				return err
			}
			debug, err := cli.GetDebug(*flags)
			if err != nil {
				return err
			}
			for _, tbl := range tbls {
				req := debugpb.ScanMvccRequest{
					FromKey: toKvMvccKey(tablecodec.GenTablePrefix(tbl.ID)),
					ToKey:   toKvMvccKey(tablecodec.GenTablePrefix(tbl.ID + 1)),
				}
				debugScanMvcc(debug, &req, func(resp *debugpb.ScanMvccResponse) {
					var item struct {
						Key   string            `json:"key"`
						Table string            `json:"table"`
						Mvcc  *kvrpcpb.MvccInfo `json:"mvcc,omitempty"`
					}
					key, err := fromKvMvccKey(resp.Key)
					if err != nil {
						logutil.BgLogger().Warn("invalid kv mvcc key", zap.Error(err))
					}
					item.Key = hex.EncodeToString(key)
					item.Table = tbl.Name
					item.Mvcc = resp.Info
					out.Dump(item)
				})
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&filter, "filter", "test.*", "Table filter.")
	return cmd
}
