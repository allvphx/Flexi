package storage

import (
	"FC/configs"
	"github.com/viney-shih/go-lock"
)

type LockEntry struct {
	lockType uint8
	txn      *DBTxn
	next     *LockEntry
	prev     *LockEntry
}

type Manager struct {
	latch     lock.Mutex
	lockType  uint8
	lockOwner uint32
	owners    *LockEntry
	ownerCnt  uint32
	from      *RowRecord
}

func lockCompatible(a, b uint8) bool {
	if a == configs.LockNone || b == configs.LockNone {
		return true
	}
	if a == configs.LockShared && b == configs.LockShared {
		return true
	}
	return false
}

func NewManager(row *RowRecord) *Manager {
	return &Manager{
		from:     row,
		owners:   nil,
		ownerCnt: 0,
		lockType: configs.LockNone,
		latch:    lock.NewCASMutex(),
	}
}

func (c *Manager) GetRowLock(lockType uint8, txn *DBTxn) uint8 {
	c.latch.Lock()
	defer c.latch.Unlock()
	// the transaction try to upgrade/repeat exclusive lock when it has obtained R/W lock.
	if lockType == configs.LockExclusive && c.owners != nil && c.owners.txn.txnID == txn.txnID {
		if c.lockType == configs.LockExclusive {
			return configs.LockSucceed
		} else if c.lockType == configs.LockShared && c.ownerCnt == 1 {
			c.lockType = configs.LockExclusive
			c.owners.lockType = configs.LockExclusive
			return configs.LockSucceed
		}
	}
	// repeat read shall be cut on the access level.
	ok := lockCompatible(lockType, c.lockType)
	if !ok {
		return configs.LockAbort
	} else {
		entry := &LockEntry{
			lockType: lockType,
			txn:      txn,
			next:     c.owners,
			prev:     nil,
		}
		if c.owners != nil {
			c.owners.prev = entry
		}
		c.owners = entry
		c.ownerCnt++
		c.lockType = lockType
	}
	return configs.LockSucceed
}

func (c *Manager) ReleaseRowLock(txn *DBTxn) {
	c.latch.Lock()
	defer c.latch.Unlock()
	var prev, cur *LockEntry = nil, nil
	for cur = c.owners; cur != nil && cur.txn.txnID != txn.txnID; cur = cur.next {
		prev = cur
	}
	if cur != nil {
		if prev != nil {
			prev.next = cur.next
		} else {
			c.owners = cur.next
		}
		if cur.next != nil {
			cur.next.prev = prev
		}
		c.ownerCnt--
		if c.ownerCnt == 0 {
			c.lockType = configs.LockNone
		}
	} else {
		// TODO: in waiter list
	}
}
