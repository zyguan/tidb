package common

import (
	"database/sql"
	"database/sql/driver"
	"math/rand"
	"net"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
)

type tidbDriver struct {
	internal mysql.MySQLDriver
}

func (drv *tidbDriver) Open(name string) (driver.Conn, error) {
	cfg, err := mysql.ParseDSN(name)
	if err != nil {
		return nil, err
	}
	host, port, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return nil, err
	}
	if strings.Contains(host, ",") {
		hosts := strings.Split(host, ",")
		ports := strings.Split(port, ",")
		idx := rand.Intn(len(hosts))
		cfg.Addr = net.JoinHostPort(hosts[idx], ports[idx%len(ports)])
		name = cfg.FormatDSN()
	}
	return drv.internal.Open(name)
}

func tryConnectTiDB(dsn string) (*sql.DB, error) {
	driverName := "tidb"
	failpoint.Inject("MockMySQLDriver", func(val failpoint.Value) {
		driverName = val.(string)
	})
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err = db.Ping(); err != nil {
		_ = db.Close()
		return nil, errors.Trace(err)
	}
	return db, nil
}

func init() {
	sql.Register("tidb", new(tidbDriver))
}
