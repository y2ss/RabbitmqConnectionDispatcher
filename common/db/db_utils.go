package db

import (
	"database/sql"
	"time"
	"context"
)

/*
if error return nil, remember call `defer context.CancelFunc()` to releases resources if operation completes before timeout elapses
*/
func PingWithTime(db *sql.DB, seconds int64) (error, context.CancelFunc) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(seconds) * time.Second)
	if err := db.PingContext(ctx); err != nil {
		cancel()
		return err, nil
	} else {
		return nil, cancel
	}
}

func Ping(db *sql.DB) (error, context.CancelFunc) {
	return PingWithTime(db, 15)
}

/*
if error return nil, remember call `defer context.CancelFunc()` to releases resources if operation completes before timeout elapses
*/
func BeginTxWithTime(db *sql.DB, seconds int64) (*sql.Tx, error, context.CancelFunc) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(seconds) * time.Second)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		cancel()
		tx.Rollback()
		return nil, err, nil
	} else {
		return tx, nil, cancel
	}
}

func BeginTx(db *sql.DB) (*sql.Tx, error, context.CancelFunc) {
	return BeginTxWithTime(db, 10)
}

/*
if error return nil, remember call `defer context.CancelFunc()` to releases resources if operation completes before timeout elapses
*/
func TxExecWithTime(tx *sql.Tx, seconds int64, query string, args ...interface{}) (sql.Result, error, context.CancelFunc) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(seconds) * time.Second)
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		cancel()
		return nil, err, nil
	} else {
		return result, nil, cancel
	}
}

func TxExec(tx *sql.Tx, query string, args ...interface{}) (sql.Result, error, context.CancelFunc) {
	return TxExecWithTime(tx,10, query, args...)
}
/*
remember call `defer context.CancelFunc()` to releases resources if operation completes before timeout elapses
*/
func TxQueryRowWithTime(tx *sql.Tx, seconds int64, query string, args ...interface{}, ) (*sql.Row, context.CancelFunc) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(seconds) * time.Second)
	row := tx.QueryRowContext(ctx, query, args...)
	return row, cancel
}

func TxQueryRow(tx *sql.Tx, query string, args ...interface{}, ) (*sql.Row, context.CancelFunc) {
	return TxQueryRowWithTime(tx,10, query, args...)
}


