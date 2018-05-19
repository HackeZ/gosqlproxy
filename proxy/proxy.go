package proxy

import (
	"database/sql"
	"fmt"
	"sort"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/hackez/gosqlproxy/conf"
)

const (
	defaultMaxLifetime = 4 * 60 * 60 // 4 hours
	defaultPoolSize    = 100
)

type Slave struct {
	db     *sql.DB
	weight int32
}

type Proxy struct {
	Master *sql.DB
	Slaves []*Slave // sort by weight from highest to lowest

	mtx *sync.Mutex
}

// New return split master-slaves databases proxy
func New(conf conf.Config) (pxy *Proxy, err error) {
	pxy = &Proxy{mtx: &sync.Mutex{}}

	master, err := openDB("mysql", conf.SchemaMaster)
	if err != nil {
		return nil, err
	}
	pxy.Master = master

	defer func() {
		if err != nil {
			master.Close() // avoid source leak if failed to open slaves databases
		}
	}()

	if conf.SchemaSlaves == nil || len(conf.SchemaSlaves) == 0 { // no available slaves databases
		pxy.Slaves = []*Slave{&Slave{master, 0}}
		return pxy, nil
	}

	for _, slave := range conf.SchemaSlaves {
		db, err := openDB("mysql", slave)
		if err != nil {
			return nil, err
		}

		pxy.Slaves = append(pxy.Slaves, &Slave{
			db:     db,
			weight: slave.Weight,
		})
	}

	// sort slaves by weight from highest to lowest
	sort.Slice(pxy.Slaves, func(i, j int) bool { return pxy.Slaves[i].weight < pxy.Slaves[j].weight })

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
	p.Master.Close()
	for _, slave := range p.Slaves {
		slave.db.Close()
	}

	return
}

func (p *Proxy) getSlave() *sql.DB {
	if len(p.Slaves) == 1 {
		return p.Slaves[0].db
	}

	return p.Slaves[0].db // TODO: get slave databases random
}
