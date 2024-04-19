package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

type Output interface {
	Dump(x any) error
	Raw(bool)
}

type output struct {
	out io.Writer
	raw bool
}

func (o *output) Raw(raw bool) { o.raw = raw }

func (o *output) Dump(x any) (err error) {
	if o.raw {
		_, err = fmt.Fprintln(o.out, x)
	} else {
		err = json.NewEncoder(o.out).Encode(x)
	}
	return
}

func newRootCmd() *cobra.Command {
	var (
		path  string
		out   = &output{out: os.Stdout}
		close func() error
	)
	cmd := &cobra.Command{
		Use:          "tidbg",
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if len(path) > 0 {
				f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
				if err != nil {
					return err
				}
				out.out = f
				close = f.Close
			}
			return nil
		},
		PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
			if close != nil {
				return close()
			}
			return nil
		},
	}
	cmd.PersistentFlags().StringVarP(&path, "output", "o", "", "output path")
	cmd.AddCommand(newEvalCmd(out))
	cmd.AddCommand(newMvccCmd(out))
	return cmd
}

func mustReplaceLogOut() {
	logOut, _, err := zap.Open("stderr")
	if err != nil {
		logutil.BgLogger().Fatal("init logging", zap.Error(err))
	}
	logger, props, err := log.InitLoggerWithWriteSyncer(&log.Config{Level: "info", File: log.FileLogConfig{}}, logOut, logOut)
	if err != nil {
		logutil.BgLogger().Fatal("init logging", zap.Error(err))
	}
	log.ReplaceGlobals(logger, props)
}

func main() {
	mustReplaceLogOut()
	cobra.EnableCommandSorting = false
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
