package proxy

import (
	"database/sql"
)

// Prepare sending raw SQL first and passing args only
// must close stmt manually after using Prepare DB connection
func (p *Proxy) Prepare(sql string) (stmt *sql.Stmt, err error) {
	db := p.GetMaster()

	return db.Prepare(sql)
}
