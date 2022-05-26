package main

import (
	"encoding/hex"
	"strconv"

	"github.com/spf13/cobra"
)

func newCodecCmd(out Output) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "codec",
		Short: "Codec utilities",
	}
	cmd.AddCommand(&cobra.Command{
		Use:   "quote",
		Short: "Quote hex keys",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var item struct {
				In  string `json:"in"`
				Out string `json:"out,omitempty"`
				Err string `json:"err,omitempty"`
			}
			for _, k := range args {
				item.In = k
				raw, err := hex.DecodeString(k)
				if err != nil {
					item.Err = err.Error()
				} else {
					item.Out = strconv.Quote(string(raw))
				}
				if err = out.Dump(item); err != nil {
					return err
				}
			}
			return nil
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "unquote",
		Short: "Unquote string literals",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var item struct {
				In  string `json:"in"`
				Out string `json:"out,omitempty"`
				Err string `json:"err,omitempty"`
			}
			for _, k := range args {
				item.In = k
				if len(k) > 0 && k[0] != '"' {
					k = `"` + k + `"`
				}
				raw, err := strconv.Unquote(k)
				if err != nil {
					item.Err = err.Error()
				} else {
					item.Out = hex.EncodeToString([]byte(raw))
				}
				if err = out.Dump(item); err != nil {
					return err
				}
			}
			return nil
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "encode-mvcc-key",
		Short: "Encode hex keys to tikv mvcc keys",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var item struct {
				In  string `json:"in"`
				Out string `json:"out,omitempty"`
				Err string `json:"err,omitempty"`
			}
			for _, k := range args {
				item.In = k
				raw, err := hex.DecodeString(k)
				if err != nil {
					item.Err = err.Error()
				} else {
					item.Out = hex.EncodeToString(toKvMvccKey(raw))
				}
				if err = out.Dump(item); err != nil {
					return err
				}
			}
			return nil
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "decode-mvcc-key",
		Short: "Decode tikv mvcc keys to hex keys",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var item struct {
				In  string `json:"in"`
				Out string `json:"out,omitempty"`
				Err string `json:"err,omitempty"`
			}
			for _, k := range args {
				item.In = k
				raw, err := hex.DecodeString(k)
				if err != nil {
					item.Err = err.Error()
				} else {
					key, err := fromKvMvccKey(raw)
					if err != nil {
						item.Err = err.Error()
					} else {
						item.Out = hex.EncodeToString(key)
					}
				}
				if err = out.Dump(item); err != nil {
					return err
				}
			}
			return nil
		},
	})
	return cmd
}
