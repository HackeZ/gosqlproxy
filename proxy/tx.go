package proxy

import (
	"database/sql"
)

type TxHandler struct {
	tx   *sql.Tx
	stmt *sql.Stmt // avoid source leak
}

// NewTx new transation in master databases
func (p *Proxy) NewTx() (*TxHandler, error) {
	tx, err := p.GetMaster().Begin()
	if err != nil {
		return nil, err
	}

	return &TxHandler{
		tx: tx,
	}, nil
}

// Query rows in transation
func (txh *TxHandler) Query(sql string, args ...interface{}) (rows *sql.Rows, err error) {
	txh.stmt, err = txh.tx.Prepare(sql)
	if err != nil {
		return nil, err
	}

	rows, err = txh.stmt.Query(args...)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

// Exec insert/update/delete in transation
func (txh *TxHandler) Exec(sql string, args ...interface{}) (affected, lastID int64, err error) {
	txh.stmt, err = txh.tx.Prepare(sql)
	if err != nil {
		return 0, 0, err
	}

	r, err := txh.stmt.Exec(args...)
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

// Commit all change in this transation
func (txh *TxHandler) Commit() error {
	err := txh.tx.Commit()
	if err != nil {
		return err
	}

	txh.stmt.Close()

	return nil
}

// Rollback all change in this transation
func (txh *TxHandler) Rollback() error {
	err := txh.tx.Rollback()
	if err != nil {
		return err
	}

	txh.stmt.Close()
	return nil
}
