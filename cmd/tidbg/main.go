package main

import (
	"encoding/json"
	"os"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

type Output interface {
	Dump(x any) error
}

type OutputFunc func(x any) error

func (f OutputFunc) Dump(x any) error { return f(x) }

func newRootCmd() *cobra.Command {
	var (
		path  string
		enc   = json.NewEncoder(os.Stdout)
		out   = OutputFunc(func(x any) error { return enc.Encode(x) })
		close func() error
		flags ClientFlags
	)
	cmd := &cobra.Command{
		Use: "tidbg",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if len(path) > 0 {
				f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
				if err != nil {
					return err
				}
				enc = json.NewEncoder(f)
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
	flags.RegisterPFlags(cmd.PersistentFlags())
	cmd.PersistentFlags().StringVarP(&path, "output", "o", "", "Output path.")
	cmd.AddCommand(newCodecCmd(out))
	cmd.AddCommand(newMvccCmd(out, &flags))
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
	if err := newRootCmd().Execute(); err != nil {
		logutil.BgLogger().Fatal("execute error", zap.Error(err))
	}
}
