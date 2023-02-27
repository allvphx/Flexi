package storage

import (
	"FC/configs"
	"context"
	"database/sql"
	_ "github.com/lib/pq"
	"strconv"
)

type SQLDB struct {
	ctx       context.Context
	db        *sql.DB
	fromTable *Table
}

type YCSBDataSQL struct {
	Key   string   `json:"key"`
	Value *RowData `json:"value"`
}

func (c *SQLDB) tryExec(sql string) {
	_, _ = c.db.Exec(sql)
}

func (c *SQLDB) mustExec(sql string) {
	_, err := c.db.Exec(sql)
	if err != nil {
		panic(err)
	}
}

func (c *SQLDB) init() {
	var err error
	c.ctx = context.TODO()
	//c.db, err = sql.Open("postgres", "postgres://postgres:flexicommit@localhost:5432/?sslmode=disable")
	//if err != nil {
	//	panic(err)
	//}
	//err = c.db.Close()
	//if err != nil {
	//	panic(err)
	//}
	//c.tryExec("create database flexi")
	c.db, err = sql.Open("postgres", "postgres://postgres:flexicommit@localhost:5432/flexi?sslmode=disable")
	if err != nil {
		panic(err)
	}
	c.db.SetMaxOpenConns(1000)
	//c.mustExec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	//c.mustExec("ALTER SYSTEM SET max_connections = 1000")
	//c.mustExec("ALTER SYSTEM SET shared_buffers = '2GB'")
	//c.mustExec("ALTER SYSTEM SET wal_level = 'minimal'")
	//c.mustExec("ALTER SYSTEM SET fsync = 'off'")
	//c.mustExec("ALTER SYSTEM SET full_page_writes = 'on'")
	//c.mustExec("SELECT pg_reload_conf()")
	c.tryExec("DROP TABLE IF EXISTS YCSB_MAIN")
	c.tryExec("CREATE TABLE YCSB_MAIN (key VARCHAR(255) PRIMARY KEY,  value VARCHAR(255))")
}

func (c *SQLDB) Insert(tableName string, key uint64, value *RowData) bool {
	rec := YCSBDataSQL{Key: strconv.FormatUint(key, 10), Value: value}
	_, err := c.db.Exec("insert into YCSB_MAIN (key, value) values ($1, $2)", rec.Key, rec.Value.String())
	if err != nil {
		panic(err)
	}
	return err == nil
}

func (c *SQLDB) Update(tableName string, key uint64, value *RowData) bool {
	rec := YCSBDataSQL{Key: strconv.FormatUint(key, 10), Value: value}
	_, err := c.db.Exec("update YCSB_MAIN set value = $2 where key = $1", rec.Key, rec.Value.String())
	return err == nil
}

func (c *SQLDB) Read(tableName string, key uint64) (*RowData, bool) {
	id := strconv.FormatUint(key, 10)
	res := RowData{}
	var value string
	err := c.db.QueryRow("select value from YCSB_MAIN where key = $1", id).Scan(&value)
	if err != nil {
		return nil, false
	}
	res.Length = 10
	res.Value = make([]interface{}, 10)
	res.fromTable = c.fromTable
	res.Value[0] = value
	return &res, err == nil
}

func (c *SQLDB) ReadTx(tx *sql.Tx, tableName string, key uint64) (*RowData, bool) {
	id := strconv.FormatUint(key, 10)
	res := RowData{}
	var value string
	err := tx.QueryRow("select value from YCSB_MAIN where key = $1", id).Scan(&value)
	if err != nil {
		return nil, false
	}
	res.Length = 10
	res.Value = make([]interface{}, 10)
	res.fromTable = c.fromTable
	res.Value[0] = value
	return &res, err == nil
}

func (c *SQLDB) Begin() *sql.Tx {
	tx, err := c.db.Begin()
	if err != nil {
		panic(err)
	}
	return tx
}

func (c *SQLDB) UpdateTX(tx *sql.Tx, tableName string, key uint64, value *RowData) bool {
	rec := YCSBDataSQL{Key: strconv.FormatUint(key, 10), Value: value}
	_, err := tx.Exec("update YCSB_MAIN set value = $2 where key = $1", rec.Key, rec.Value.String())
	return err == nil
}

func (c *Shard) ReadTxnPostgres(tableName string, txnID uint32, key uint64) (*RowData, bool) {
	configs.TPrintf("TXN" + strconv.FormatUint(uint64(txnID), 10) + ": reading data on " +
		c.shardID + " " + tableName + ":" + strconv.Itoa(int(key)))
	v, ok := c.txnPool.Load(txnID)
	if !ok {
		configs.Warn(ok, "the transaction has been aborted.")
		configs.TPrintf("TXN" + strconv.FormatUint(uint64(txnID), 10) + ": transaction aborted on shard " +
			c.shardID + " " + tableName + ":" + strconv.Itoa(int(key)))
		// the transaction branch could have been aborted due to the abort of another participant.
		// in this case, we ignore it.
		return nil, false
	}
	tx := v.(*DBTxn)
	configs.Assert(tx.txnID == txnID, "different transaction running")
	return c.db.ReadTx(tx.sqlTX, tableName, key)
}

func (c *Shard) UpdateTxnPostgres(tableName string, txnID uint32, key uint64, value *RowData) bool {
	configs.TPrintf("TXN" + strconv.FormatUint(uint64(txnID), 10) + ": update Value on shard " + c.shardID + " " + tableName + ":" + strconv.Itoa(int(key)) + ":" + value.String())
	v, ok := c.txnPool.Load(txnID)
	if !ok {
		configs.Warn(ok, "the transaction has been aborted.")
		// the transaction branch could have been aborted due to the abort of another participant.
		// in this case, we ignore it.
		return false
	}
	tx := v.(*DBTxn)
	configs.Assert(tx.txnID == txnID, "different transaction running")
	return c.db.UpdateTX(tx.sqlTX, tableName, key, value)
}

func (c *Shard) RollBackPostgres(txnID uint32) bool {
	v, ok := c.txnPool.Load(txnID)
	if !ok {
		configs.Warn(ok, "the transaction has been aborted.")
		return true
	}
	tx := v.(*DBTxn)
	configs.Assert(tx.txnID == txnID, "different transaction running")
	if !tx.TryFinish() {
		return true
	}
	tx.TxnState = txnAborted
	err := tx.sqlTX.Rollback()
	if err != nil {
		panic(err)
	}
	c.log.writeTxnState(tx)
	c.txnPool.Delete(txnID)
	return true
}

func (c *Shard) CommitPostgres(txnID uint32) bool {
	v, ok := c.txnPool.Load(txnID)
	configs.Warn(ok, "the transaction has finished before commit on this node.")
	if !ok {
		return true
	}
	tx := v.(*DBTxn)
	configs.Assert(tx.txnID == txnID, "different transaction running")
	if !tx.TryFinish() {
		return true
	}
	tx.TxnState = txnCommitted
	if tx.TxnState == txnExecution {
		// for a local transaction, there is no prepare phase that persists the redo logs.
		c.log.writeRedoLog4Txn(tx)
	}
	c.log.writeTxnState(tx)
	err := tx.sqlTX.Commit()
	if err != nil {
		panic(err)
	}
	c.txnPool.Delete(txnID)
	return true
}
