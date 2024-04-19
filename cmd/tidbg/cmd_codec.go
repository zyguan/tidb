package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"text/template"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/spf13/cobra"
)

func newCodecCmd(out Output) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "codec",
		Short: "Codec utilities",
	}
	cmd.AddCommand(newCodecEvalCmd(out))
	return cmd
}

func newCodecEvalCmd(out Output) *cobra.Command {
	var (
		format       string
		oldCollation bool
	)
	cmd := &cobra.Command{
		Use:   "eval <template>",
		Short: "Eval a template that can interact with codec functions",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			buf := new(bytes.Buffer)
			tmpl, err := template.New("codec").Funcs(template.FuncMap{
				"quote":   strconv.Quote,
				"unquote": strconv.Unquote,
				"trim":    strings.TrimSpace,
				"hex": func(s string) string {
					return hex.EncodeToString([]byte(s))
				},
				"unhex": func(s string) (string, error) {
					return tmplBytesError(hex.DecodeString(s))
				},
				"key": func(s string) string {
					return string(codec.EncodeBytes(nil, []byte(s)))
				},
				"unkey": func(s string) (string, error) {
					_, v, err := codec.DecodeBytes([]byte(s), nil)
					return tmplBytesError(v, err)
				},
				"i64": func(i int64) string {
					return string(codec.EncodeInt(nil, i))
				},
				"u64": func(i uint64) string {
					return string(codec.EncodeUint(nil, i))
				},
				"signed": func(i int64) (string, error) {
					return tmplEncodeKey(types.NewIntDatum(i))
				},
				"unsigned": func(i uint64) (string, error) {
					return tmplEncodeKey(types.NewUintDatum(i))
				},
				"float": func(f float64) (string, error) {
					return tmplEncodeKey(types.NewFloat64Datum(f))
				},
				"text": func(s string) (string, error) {
					return tmplEncodeKey(types.NewStringDatum(s))
				},
				"blob": func(s string) (string, error) {
					return tmplEncodeKey(types.NewBytesDatum([]byte(s)))
				},
				"null": func() (string, error) {
					return tmplEncodeKey(types.Datum{})
				},
			}).Parse(args[0])
			if err != nil {
				return err
			}

			if oldCollation {
				collate.SetNewCollationEnabledForTest(false)
			}

			input := ""
			if fi, _ := os.Stdin.Stat(); (fi.Mode() & os.ModeCharDevice) == 0 {
				in, err := io.ReadAll(os.Stdin)
				if err != nil {
					return err
				}
				input = strings.TrimSpace(string(in))
			}

			err = tmpl.Execute(buf, input)

			if err != nil {
				return err
			}
			out.Raw(true)
			switch format {
			case "key":
				out.Dump(hex.EncodeToString(codec.EncodeBytes(nil, buf.Bytes())))
			case "hex":
				out.Dump(hex.EncodeToString(buf.Bytes()))
			case "raw":
				out.Dump(buf.String())
			default:
				return fmt.Errorf("unknown format %q", format)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&format, "format", "f", "key", "Output format, one of: key, hex, raw.")
	cmd.Flags().BoolVar(&oldCollation, "old-collation", false, "Use old collation.")
	return cmd
}

func tmplBytesError(bs []byte, err error) (string, error) {
	if err != nil {
		return "", err
	}
	return string(bs), nil
}

func tmplEncodeKey(d types.Datum) (string, error) {
	return tmplBytesError(codec.EncodeKey(nil, nil, d))
}
