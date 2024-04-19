package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"text/template"
	"time"
	"unicode"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
)

var builtinFuncs = template.FuncMap{
	"trim":    strings.TrimSpace,
	"quote":   quote,
	"unquote": unquote,

	// functions for parsing domain specific data
	"decodePlan":       plancodec.DecodePlan,
	"decodeBinaryPlan": plancodec.DecodeBinaryPlan,
	"showCreateTable":  tidbShowCreateTable,

	// functions for handling tso
	"tsoPhysical": oracle.ExtractPhysical,
	"tsoLogical":  oracle.ExtractLogical,
	"tsoCompose":  oracle.ComposeTS,
	"tsoTime": func(tso uint64) string {
		t := oracle.GetTimeFromTS(tso)
		return t.Format(time.RFC3339Nano)
	},

	// functions for common binary codec
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

	// functions for encoding datums
	"signed": func(i int64) (string, error) {
		return tmplEncodeDatum(types.NewIntDatum(i))
	},
	"unsigned": func(i uint64) (string, error) {
		return tmplEncodeDatum(types.NewUintDatum(i))
	},
	"float": func(f float64) (string, error) {
		return tmplEncodeDatum(types.NewFloat64Datum(f))
	},
	"text": func(s string) (string, error) {
		return tmplEncodeDatum(types.NewStringDatum(s))
	},
	"blob": func(s string) (string, error) {
		return tmplEncodeDatum(types.NewBytesDatum([]byte(s)))
	},
	"year": func(y int64) (string, error) {
		return tmplEncodeTimeDatum(types.ParseTimeFromYear(y))
	},
	"datetime": func(s string) (string, error) {
		return tmplEncodeTimeDatum(types.ParseDatetime(types.DefaultStmtNoWarningContext, s))
	},
	"timestamp": func(s string) (string, error) {
		return tmplEncodeTimeDatum(types.ParseTimestamp(types.DefaultStmtNoWarningContext, s))
	},
	"date": func(s string) (string, error) {
		return tmplEncodeTimeDatum(types.ParseDate(types.DefaultStmtNoWarningContext, s))
	},
	"time": func(s string) (string, error) {
		dur, null, err := types.ParseDuration(types.DefaultStmtNoWarningContext, s, types.GetFsp(s))
		if err != nil {
			return "", err
		}
		if null {
			return tmplEncodeDatum(types.Datum{})
		}
		return tmplEncodeDatum(types.NewDurationDatum(dur))
	},
	"decimal": func(s string) (string, error) {
		var dec types.MyDecimal
		if err := dec.FromString([]byte(s)); err != nil {
			return "", err
		}
		return tmplEncodeDatum(types.NewDecimalDatum(&dec))
	},
	"enum": func(name, elems string) (string, error) {
		x, err := types.ParseEnum(strings.Split(elems, ","), name, mysql.DefaultCollationName)
		if err != nil {
			return "", err
		}
		return tmplEncodeDatum(types.NewMysqlEnumDatum(x))
	},
	"set": func(name, elems string) (string, error) {
		x, err := types.ParseSet(strings.Split(elems, ","), name, mysql.DefaultCollationName)
		if err != nil {
			return "", err
		}
		return tmplEncodeDatum(types.NewMysqlSetDatum(x, mysql.DefaultCollationName))
	},
	"bit": func(s string) (string, error) {
		x, err := types.ParseBitStr(s)
		if err != nil {
			return "", err
		}
		return tmplEncodeDatum(types.NewBinaryLiteralDatum(x))
	},
	"json": func(s string) (string, error) {
		x, err := types.ParseBinaryJSONFromString(s)
		if err != nil {
			return "", err
		}
		return tmplEncodeDatum(types.NewJSONDatum(x))
	},
	"null": func() (string, error) {
		return tmplEncodeDatum(types.Datum{})
	},
}

func newEvalCmd(out Output) *cobra.Command {
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
			tmpl, err := template.New("codec").Funcs(builtinFuncs).Parse(args[0])
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

			if format == "" {
				format = "raw"
				for _, r := range buf.String() {
					if !unicode.IsPrint(r) && !unicode.IsSpace(r) {
						format = "hex"
						break
					}
				}
			}

			out.Raw(true)
			switch format {
			case "raw":
				out.Dump(buf.String())
			case "hex":
				out.Dump(hex.EncodeToString(buf.Bytes()))
			case "key":
				out.Dump(hex.EncodeToString(codec.EncodeBytes(nil, buf.Bytes())))
			case "quote":
				out.Dump(quote(string(codec.EncodeBytes(nil, buf.Bytes()))))
			default:
				return fmt.Errorf("unknown format %q", format)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&format, "format", "f", "", "output format: raw, hex, key, quote")
	cmd.Flags().BoolVar(&oldCollation, "old-collation", false, "use old collation")
	return cmd
}

func tmplBytesError(bs []byte, err error) (string, error) {
	if err != nil {
		return "", err
	}
	return string(bs), nil
}

func tmplEncodeDatum(d types.Datum) (string, error) {
	return tmplBytesError(codec.EncodeKey(time.UTC, nil, d))
}

func tmplEncodeTimeDatum(t types.Time, err error) (string, error) {
	if err != nil {
		return "", err
	}
	return tmplEncodeDatum(types.NewTimeDatum(t))
}

func tidbShowCreateTable(s string) (string, error) {
	var (
		tbl   model.TableInfo
		alloc autoid.Allocators
		dec   = json.NewDecoder(strings.NewReader(s))
		ctx   = mock.NewContext()
		buf   = new(bytes.Buffer)
	)
	for {
		err := dec.Decode(&tbl)
		if err == io.EOF {
			return buf.String(), nil
		} else if err != nil {
			return "", err
		}
		if buf.Len() > 0 {
			buf.WriteString("\n")
		}
		err = executor.ConstructResultOfShowCreateTable(ctx, &tbl, alloc, buf)
		if err != nil {
			return "", err
		}
		buf.WriteString(";")
	}
}
