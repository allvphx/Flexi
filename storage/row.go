package storage

import (
	"FC/configs"
	"sync/atomic"
)

type Table struct {
	primaryIndex          *BTree
	tableName             string
	attributesNum         int
	autoIncreasingPrimary int32
}

func (tab *Table) GenPrimaryKey() int {
	return int(atomic.AddInt32(&tab.autoIncreasingPrimary, 1))
}

type Key uint64
type Access uint8

type RowData struct {
	Length    uint          `bson:"length"`
	Value     []interface{} `bson:"value"`
	fromTable *Table
}

type RowRecord struct {
	Epoch      uint64
	RowID      Key
	FromTable  *Table
	PrimaryKey Key
	Data       *RowData // attributes get merged into one string
	manager    *Manager
}

func NewRowData(tb *Table) *RowData {
	res := &RowData{}
	res.fromTable = tb
	if tb != nil {
		res.Length = uint(tb.attributesNum)
		res.Value = make([]interface{}, tb.attributesNum)
	} else {
		res.Length = 1
		res.Value = make([]interface{}, 1)
	}
	return res
}

func NewRowDataWithLength(len int) *RowData {
	res := &RowData{}
	res.fromTable = nil
	res.Length = uint(len)
	res.Value = make([]interface{}, len)
	return res
}

func NewRowRecord(table *Table, primaryKey Key, rowKey Key) *RowRecord {
	res := &RowRecord{
		RowID:      rowKey,
		FromTable:  table,
		PrimaryKey: primaryKey,
		Data:       NewRowData(table),
	}
	res.manager = NewManager(res)
	return res
}

func (r *RowRecord) Copy(row *RowRecord) {
	r.Data = row.Data
}

func (r *RowRecord) ReturnRow(typ uint8, txn *DBTxn, row *RowRecord) {
	if row == nil {
		// this could happen when the transaction could be concurrently committed/aborted.
		return
	}
	configs.Assert(typ != TxnRollBack || (row == r && typ == TxnRollBack), "incorrect return row call")
	if typ == TxnWrite {
		r.Data = row.Data
	}
	r.manager.ReleaseRowLock(txn)
}

func (c *RowData) SetAttribute(idx uint, value interface{}) {
	configs.Assert(idx < c.Length && idx >= 0, "attribute access out of range")
	c.Value[idx] = value
}

func (c *RowData) GetAttribute(idx uint) interface{} {
	configs.Assert(idx < c.Length && idx >= 0, "attribute access out of range")
	return c.Value[idx]
}

func (c *RowData) String() string {
	return "Dummy data"
}

func (c *RowRecord) GetValue(idx uint) interface{} {
	return c.Data.GetAttribute(idx)
}

func (c *RowRecord) SetValue(idx uint, value interface{}) {
	c.Data.SetAttribute(idx, value)
}

func (r *RowRecord) GetRecord(typ uint8, txn *DBTxn) *RowRecord {
	var res *RowRecord = nil
	lc := configs.LockExclusive
	if typ == TxnRead || typ == TxnScan {
		lc = configs.LockShared
	}
	rc := r.manager.GetRowLock(uint8(lc), txn)
	if rc == configs.LockSucceed {
		res = NewRowRecord(r.FromTable, r.PrimaryKey, r.PrimaryKey)
		res.Data = r.Data
	} else if rc == configs.LockAbort {
		res = nil
	} else if rc == configs.LockWait {
	}
	return res
}
