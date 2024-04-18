package main

import (
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/spf13/cobra"
)

func newPlanCmd(out Output) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Plan utilities",
	}
	cmd.AddCommand(newPlanDecodeCmd(out))
	return cmd
}

func newPlanDecodeCmd(out Output) *cobra.Command {
	var (
		bin bool
	)
	cmd := &cobra.Command{
		Use:   "decode",
		Short: "Decode plan",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			decode := plancodec.DecodePlan
			if bin {
				decode = plancodec.DecodeBinaryPlan
			}
			txt, err := decode(args[0])
			if err != nil {
				return err
			}
			out.Raw(true)
			out.Dump(txt)
			return nil
		},
	}
	cmd.Flags().BoolVarP(&bin, "binary", "b", false, "Decode binary plan.")
	return cmd
}
