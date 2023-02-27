package storage

import (
	"FC/configs"
	"github.com/barkimedes/go-deepcopy"
	"testing"
	"time"
)

const TxnPerThread = 100000
const ThreadNumber = 16

func Test2PLNoWait(t *testing.T) {
	n := ThreadNumber * TxnPerThread
	row := NewRowRecord(nil, 1, 1)
	ch := make(chan bool)
	for i := 0; i < ThreadNumber; i++ {
		go func(i int, ch chan bool) {
			txn := NewTxn()
			for j := i; j < n; j += ThreadNumber {
				txn.txnID = uint32(j)
				rc := row.GetRecord(TxnRead, txn)
				if rc != nil {
					rc = row.GetRecord(TxnWrite, txn)
					if rc != nil {
						newRow := deepcopy.MustAnything(row).(*RowRecord)
						newRow.SetValue(configs.F0, time.Now().String())
						row.ReturnRow(TxnWrite, txn, newRow)
						row.ReturnRow(TxnRead, txn, row)
					} else {
						row.ReturnRow(TxnWrite, txn, row)
					}
				} else {
					row.ReturnRow(TxnRead, txn, row)
				}
				newRow := deepcopy.MustAnything(row).(*RowRecord)
				newRow.SetValue(configs.F0, time.Now().String())
				row.ReturnRow(TxnWrite, txn, newRow)
				row.ReturnRow(TxnRead, txn, row)
			}

			ch <- true
		}(i, ch)
	}
	for i := 0; i < ThreadNumber; i++ {
		<-ch
	}
}
