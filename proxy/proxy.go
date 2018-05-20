package proxy

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/hackez/gosqlproxy/conf"
	"github.com/hackez/weight"
)

const (
	defaultMaxLifetime = 4 * 60 * 60 // 4 hours
	defaultPoolSize    = 100
)

// Proxy route write/read request to different instance database
type Proxy struct {
	master *sql.DB
	slave  weight.Weight

	mtx *sync.Mutex
}

// New return split master-slaves databases proxy
func New(conf conf.Config) (pxy *Proxy, err error) {
	pxy = &Proxy{mtx: &sync.Mutex{}}

	master, err := openDB("mysql", conf.SchemaMaster)
	if err != nil {
		return nil, err
	}
	pxy.master = master

	defer func() {
		if err != nil {
			master.Close() // avoid source leak if failed to open slaves databases
		}
	}()

	// no available slaves databases
	if conf.SchemaSlaves == nil || len(conf.SchemaSlaves) == 0 {
		return pxy, nil
	}

	pxy.slave = &weight.SW{} // use smooth round-robin weight
	for _, slave := range conf.SchemaSlaves {
		db, err := openDB("mysql", slave)
		if err != nil {
			return nil, err
		}

		err = pxy.slave.Add(slave.DSN(), db, slave.Weight)
		if err != nil {
			return pxy, err
		}
	}

	return pxy, nil
}

func openDB(driver string, cfg conf.Schema) (*sql.DB, error) {
	db, err := sql.Open(driver, cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("failed to open schema: %v", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to ping schema: %v", err)
	}

	lt := func(sec int32) time.Duration {
		if sec > 0 {
			return time.Duration(sec)
		}

		return time.Duration(defaultMaxLifetime)
	}
	db.SetConnMaxLifetime(lt(cfg.PacemakerSecond) * time.Second)

	ps := func() int {
		if cfg.PoolSize > 0 {
			return int(cfg.PoolSize)
		}

		return defaultPoolSize
	}()
	db.SetMaxOpenConns(ps)
	db.SetMaxIdleConns(ps)

	return db, nil
}

// Shutdown close all available databases connect
func (p *Proxy) Shutdown() {
	p.mtx.Lock()

	// close all db instants connect
	p.master.Close()
	p.slave.Close(func(db interface{}) error {
		return db.(*sql.DB).Close()
	})

	p.mtx.Unlock()
	return
}

// GetMaster return master instance database
func (p *Proxy) GetMaster() *sql.DB {
	return p.master
}

// GetSlave return slave instance database
func (p *Proxy) GetSlave() *sql.DB {
	return p.slave.Next().(*sql.DB)
}
