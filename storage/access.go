package storage

const (
	TxnScan     = 0
	TxnRead     = 1
	TxnWrite    = 2
	TxnRollBack = 3
)

type txnAccess struct {
	Origin     *RowRecord
	Local      *RowRecord
	AccessType uint8
}
