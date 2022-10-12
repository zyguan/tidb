package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"io"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
)

var (
	logger = log.New(os.Stdout, "", 0)
	global struct {
		Time    time.Duration
		Threads int
		DSN     string
	}
)

type TxRecord struct {
	Time    string `json:"time"`
	Conn    int64  `json:"conn"`
	BeginAt string `json:"begin_at"`
	Dur     string `json:"dur"`
	Stage   string `json:"stage"`
	Error   string `json:"error"`
}

func init() {
	mysql.SetLogger(log.New(io.Discard, "", 0))
	flag.DurationVar(&global.Time, "time", time.Hour, "")
	flag.IntVar(&global.Threads, "threads", 50, "")
	flag.StringVar(&global.DSN, "dsn", "root:@tcp(127.0.0.1:4000)/test", "")
}

func main() {
	flag.Parse()
	ctx := context.Background()
	db, err := sql.Open("mysql", global.DSN)
	if err != nil {
		panic(err)
	}
	bootstrap(db)
	for i := 0; i < global.Threads; i++ {
		go runLoop(ctx, db)
	}
	time.Sleep(global.Time)
}

func bootstrap(db *sql.DB) {
	_, err := db.Exec("create table if not exists event (ts bigint, tx bigint, conn bigint, msg text)")
	if err != nil {
		panic(err)
	}
}

func runLoop(ctx context.Context, db *sql.DB) {
	time.Sleep(time.Duration(rand.Intn(20 * int(time.Second))))
	for {
		conn, err := db.Conn(ctx)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		var connID int64
		if err := conn.QueryRowContext(ctx, "select connection_id()").Scan(&connID); err != nil {
			conn.Close()
			time.Sleep(time.Second)
			continue
		}
		startTime := time.Now()
		stage, err := doTx(ctx, conn, randThinkTime())
		now := time.Now()
		duration := now.Sub(startTime)
		if err != nil && duration < 30*time.Second {
			msg, _ := json.Marshal(TxRecord{
				Conn:    connID,
				Time:    now.Format("2006-01-02T15:04:05.000Z07:00"),
				BeginAt: startTime.Format("2006-01-02T15:04:05.000Z07:00"),
				Dur:     duration.String(),
				Stage:   stage,
				Error:   err.Error(),
			})
			logger.Print(string(msg))
		}
		conn.Close()
	}
}

func doTx(ctx context.Context, c *sql.Conn, thinkTime time.Duration) (string, error) {
	if _, err := c.ExecContext(ctx, "begin"); err != nil {
		return "begin", err
	}
	if _, err := c.ExecContext(ctx, "insert into event values (?, @@tidb_current_ts, connection_id(), ?)", time.Now().UnixMilli(), "insert-1"); err != nil {
		return "insert-1", err
	}
	if thinkTime > 0 {
		time.Sleep(time.Duration(thinkTime))
	}
	if _, err := c.ExecContext(ctx, "insert into event values (?, @@tidb_current_ts, connection_id(), ?)", time.Now().UnixMilli(), "insert-2"); err != nil {
		return "insert-2", err
	}
	if _, err := c.ExecContext(ctx, "commit"); err != nil {
		return "commit", err
	}
	return "done", nil
}

func randThinkTime() time.Duration {
	return time.Duration(float64(20*time.Second)*rand.NormFloat64() + float64(20*time.Second))
}
