package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/spf13/pflag"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc"

	_ "github.com/go-sql-driver/mysql"
)

type ClientFlags struct {
	User string
	Pass string
	Host string
	Port int
	PD   string
	KV   string
}

func (cfg *ClientFlags) RegisterPFlags(flags *pflag.FlagSet) {
	flags.StringVarP(&cfg.User, "user", "u", "root", "Username to connect to the database.")
	flags.StringVarP(&cfg.Pass, "password", "p", "", "Password to connect to the database.")
	flags.StringVarP(&cfg.Host, "host", "h", "127.0.0.1", "Host of the database.")
	flags.IntVarP(&cfg.Port, "port", "P", 4000, "Port number to use for connection.")
	flags.StringVar(&cfg.PD, "pd", "", "PD address to connect. (eg. 127.0.0.1:2379)")
	flags.StringVar(&cfg.KV, "kv", "", "TiKV address to connect. (eg. 127.0.0.1:20160)")
	flags.Bool("help", false, "Help message.")
}

type ClientSet struct {
	pd    pd.Client
	kv    *tikv.KVStore
	db    *sql.DB
	debug debugpb.DebugClient

	endpoints map[string][]string
}

func (cli *ClientSet) endpointsOf(flags ClientFlags, comp string) ([]string, error) {
	if comp == "pd" && len(flags.PD) > 0 {
		return []string{flags.PD}, nil
	}
	if comp == "tikv" && len(flags.KV) > 0 {
		return []string{flags.KV}, nil
	}
	if cli.endpoints == nil {
		cli.endpoints = make(map[string][]string)
	}
	if lst := cli.endpoints[comp]; len(lst) > 0 {
		return lst, nil
	}
	db, err := cli.GetDB(flags)
	if err != nil {
		return nil, err
	}
	as, err := dbListInstances(db, comp)
	if err != nil {
		return nil, err
	}
	cli.endpoints[comp] = as
	return as, nil
}

func (cli *ClientSet) GetDB(flags ClientFlags) (*sql.DB, error) {
	if cli.db != nil {
		return cli.db, nil
	}
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/", flags.User, flags.Pass, flags.Host, flags.Port))
	if err != nil {
		return nil, err
	}
	cli.db = db
	return db, nil
}

func (cli *ClientSet) GetPD(flags ClientFlags) (pd.Client, error) {
	if cli.pd != nil {
		return cli.pd, nil
	}
	pds, err := cli.endpointsOf(flags, "pd")
	if err != nil {
		return nil, err
	}
	c, err := tikv.NewPDClient(pds)
	if err != nil {
		return nil, err
	}
	cli.pd = c
	return c, nil
}

func (cli *ClientSet) GetKV(flags ClientFlags) (*tikv.KVStore, error) {
	if cli.kv != nil {
		return cli.kv, nil
	}
	c, err := cli.GetPD(flags)
	if err != nil {
		return nil, err
	}
	pds, err := cli.endpointsOf(flags, "pd")
	if err != nil {
		return nil, err
	}
	spkv, err := tikv.NewEtcdSafePointKV(pds, nil)
	if err != nil {
		return nil, err
	}
	uuid := fmt.Sprintf("tikv-%v", c.GetClusterID(context.TODO()))
	if err != nil {
		return nil, err
	}
	s, err := tikv.NewKVStore(uuid, c, spkv, tikv.NewRPCClient())
	if err != nil {
		return nil, err
	}
	cli.kv = s
	return s, nil
}

func (cli *ClientSet) GetDebug(flags ClientFlags) (debugpb.DebugClient, error) {
	if cli.debug != nil {
		return cli.debug, nil
	}
	kvs, err := cli.endpointsOf(flags, "tikv")
	if err != nil {
		return nil, err
	}
	if len(kvs) == 0 {
		return nil, errors.New("kv endpoint is required")
	}
	cc, err := grpc.Dial(kvs[0], grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	cli.debug = debugpb.NewDebugClient(cc)
	return cli.debug, nil
}
