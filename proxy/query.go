package proxy

import (
	"database/sql"
)

// Query rows on slave databases
func (p *Proxy) Query(sql string, args ...interface{}) (*sql.Rows, error) {
	db := p.getSlave()

	stmt, err := db.Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}

	return rows, nil
}
