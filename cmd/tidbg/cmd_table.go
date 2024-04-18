package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/spf13/cobra"
)

func newTableCmd(out Output) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "table",
		Short: "table utilities",
	}
	cmd.AddCommand(newTableShowDDLCmd(out))
	return cmd
}

func newTableShowDDLCmd(out Output) *cobra.Command {
	var (
		input string
	)
	cmd := &cobra.Command{
		Use:   "show-ddl",
		Short: "Show create table DDL from schema json",
		RunE: func(cmd *cobra.Command, args []string) error {
			var (
				tbl   model.TableInfo
				alloc autoid.Allocators
				dec   = json.NewDecoder(os.Stdin)
				ctx   = mock.NewContext()
				buf   = new(bytes.Buffer)
			)
			if input != "" {
				f, err := os.Open(input)
				if err != nil {
					return err
				}
				defer f.Close()
				dec = json.NewDecoder(f)

			}
			for {
				err := dec.Decode(&tbl)
				if err == io.EOF {
					return nil
				} else if err != nil {
					return err
				}
				err = executor.ConstructResultOfShowCreateTable(ctx, &tbl, alloc, buf)
				if err != nil {
					return err
				}
				out.Raw(true)
				out.Dump(buf.String() + ";")
				buf.Reset()
			}
		},
	}
	cmd.Flags().StringVarP(&input, "input", "i", "", "Input schema json file.")
	return cmd
}
