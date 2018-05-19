package proxy

// Exec insert/update/delete in master databases
func (p *Proxy) Exec(sql string, args ...interface{}) (affected, lastID int64, err error) {
	db := p.Master

	stmt, err := db.Prepare(sql)
	if err != nil {
		return 0, 0, err
	}
	defer stmt.Close()

	r, err := stmt.Exec(args...)
	if err != nil {
		return 0, 0, err
	}

	affected, err = r.RowsAffected()
	if err != nil {
		return 0, 0, err
	}

	lastID, err = r.LastInsertId()
	if err != nil {
		return 0, 0, err
	}

	return affected, lastID, nil
}
