package participant

import (
	"FC/configs"
	"FC/network"
	"FC/network/detector"
	"FC/storage"
	"fmt"
	"strconv"
	"time"
)

const (
	No  = 0
	Yes = 1
)

// TXNBranch is used to handle one specific transaction.
type TXNBranch struct {
	Kv           *storage.Shard
	Res          *detector.KvRes
	from         *Manager
	OptList      []storage.TXOpt
	voteReceived []bool
	Vote         bool
	TID          uint64
}

func NewParticipantBranch(msg *network.CoordinatorGossip, kv *storage.Shard, manager *Manager) *TXNBranch {
	res := &TXNBranch{
		TID:          msg.TxnID,
		Kv:           kv,
		Res:          detector.NewKvRes(int(msg.TxnID), kv.GetID()),
		from:         manager,
		OptList:      msg.OptList,
		Vote:         false,
		voteReceived: make([]bool, 0),
	}
	res.pruneRepeatedOperations()
	return res
}

func (c *TXNBranch) PreCommit() bool {
	if configs.EnableReplication {
		return c.Kv.PreCommitAsync(uint32(c.TID))
	} else {
		c.Kv.PreCommit(uint32(c.TID))
		return true
	}
}

func (c *TXNBranch) pruneRepeatedOperations() {
	tempOptList := make([]storage.TXOpt, 0)
	vis := make(map[string]bool)
	for i := len(c.OptList) - 1; i >= 0; i-- {
		// keep the last write operation for each key
		op := c.OptList[i]
		if op.Type == storage.ReadOpt {
			continue
		}
		s := op.Shard + ":" + op.Table + ":" + strconv.Itoa(int(op.Key)) + ":" + strconv.FormatUint(uint64(op.Type), 10)
		if !vis[s] {
			vis[s] = true
			tempOptList = append(tempOptList, op)
		}
	}
	for i, ln := 0, len(c.OptList); i < ln; i++ {
		// keep the first read operation for each key
		op := c.OptList[i]
		if op.Type == storage.UpdateOpt {
			continue
		}
		s := op.Shard + ":" + op.Table + ":" + strconv.Itoa(int(op.Key)) + ":" + strconv.FormatUint(uint64(op.Type), 10)
		if !vis[s] {
			vis[s] = true
			tempOptList = append(tempOptList, op)
		}
	}
	for i, ln := 0, len(tempOptList); i < ln/2; i++ {
		j := ln - i - 1
		tempOptList[i], tempOptList[j] = tempOptList[j], tempOptList[i]
	}
	c.OptList = tempOptList
}

// ProposeFCFF handles the Propose Phase of FCff-FF, safe and does not break atomicity.
// This is faster but could suffer from crash failure: single node crash could block the whole system.
// probabilistic analysis reveals that it shall always be faster than 2PC.
func (c *TXNBranch) ProposeFCFF(msg *network.CoordinatorGossip, sent time.Time) *detector.KvRes {
	if !c.GetVote() {
		// if vote to abort, it will abort regardless of the ACP used.
		configs.TPrintf("TXN" + strconv.FormatUint(c.TID, 10) + ": " + c.from.stmt.address + " abort for PreWrite")
		c.Res.SetSelfResult(false, false, true)
		c.from.broadCastVote(msg, No, msg.ShardID, msg.ParticipantAddresses, sent)
		configs.Assert(c.from.Abort(msg), "impossible case, abort should not fail with all locks")
	} else {
		configs.DPrintf("TXN" + strconv.FormatUint(msg.TxnID, 10) + ": " + "Yes Voting From " + c.from.stmt.address)
		hd := c.from.createIfNotExistMsgPool(msg.ShardID, msg.TxnID, len(msg.ParticipantAddresses))
		rem := c.from.stmt.GetNetworkTimeOut(msg.ParticipantAddresses)
		configs.LPrintf("TXN" + strconv.FormatUint(msg.TxnID, 10) + ": " + " participant side timeout = " + rem.String())
		c.from.broadCastVote(msg, Yes, msg.ShardID, msg.ParticipantAddresses, sent)
		select {
		case <-time.After(rem):
			configs.TPrintf("TXN" + strconv.FormatUint(c.TID, 10) + ": " + c.from.stmt.address + " auto commit")
			c.Res.SetSelfResult(true, true, false)
			return c.Res
		case <-c.from.stmt.ctx.Done():
			configs.TPrintf("TXN" + strconv.FormatUint(c.TID, 10) + ": " + c.from.stmt.address + " abort for ctx done")
			c.Res.SetSelfResult(true, false, false)
			return c.Res
		case <-hd.finish:
			if hd.canAbort {
				c.Abort()
				c.Res.SetSelfResult(true, false, true)
				return c.Res
			} else {
				c.Commit()
				c.Res.SetSelfResult(true, true, true)
				return c.Res
			}
		}
	}
	return c.Res
}

// ProposeFCCF handles the Propose Phase of FCff-CF, safe and does not break atomicity.
// This is faster but could suffer from network delay: network delay could abort the whole system.
func (c *TXNBranch) ProposeFCCF(msg *network.CoordinatorGossip, sent time.Time) *detector.KvRes {
	if !c.GetVote() {
		// if vote to abort, it will abort regardless of the ACP used.
		configs.TPrintf("TXN" + strconv.FormatUint(c.TID, 10) + ": " + c.from.stmt.address + " abort for PreWrite")
		c.Res.SetSelfResult(false, false, true)
		c.from.broadCastVote(msg, No, msg.ShardID, msg.ParticipantAddresses, sent)
		msg.Protocol = configs.TwoPC // do not broadcast Abort decision regarding No vote.
		configs.Assert(c.from.Abort(msg), "impossible case, abort should not fail with all locks")
		msg.Protocol = configs.FCcf
	} else {
		configs.DPrintf("TXN" + strconv.FormatUint(msg.TxnID, 10) + ": " + "Yes Voting From " + c.from.stmt.address)
		hd := c.from.createIfNotExistMsgPool(msg.ShardID, msg.TxnID, len(msg.ParticipantAddresses))
		rem := c.from.stmt.GetNetworkTimeOut(msg.ParticipantAddresses)
		configs.LPrintf("TXN" + strconv.FormatUint(msg.TxnID, 10) + ": " + " participant side timeout = " + rem.String())
		c.from.broadCastVote(msg, Yes, msg.ShardID, msg.ParticipantAddresses, sent)
		select {
		case <-time.After(rem):
			// abort directly after the timeout window.
			configs.TPrintf("TXN" + strconv.FormatUint(c.TID, 10) + ": " + c.from.stmt.address + " auto commit")
			c.Res.SetSelfResult(true, false, true)
			configs.Assert(c.from.Abort(msg), "impossible case, abort should not fail with all locks")
			return c.Res
		case <-c.from.stmt.ctx.Done():
			configs.TPrintf("TXN" + strconv.FormatUint(c.TID, 10) + ": " + c.from.stmt.address + " abort for ctx done")
			c.Res.SetSelfResult(true, false, true)
			configs.Assert(c.from.Abort(msg), "impossible case, abort should not fail with all locks")
			return c.Res
		case <-hd.finish:
			if hd.canAbort {
				c.Abort()
				c.Res.SetSelfResult(true, false, true)
				return c.Res
			} else {
				c.Res.SetSelfResult(true, true, false)
				return c.Res
			}
		}
	}
	return c.Res
}

// ProposeFCFFNotSafe pre-commit a transaction with failure-free level, could break atomicity.
// abort gets directly finished, but the commit shall be performed in epoch to guarantee safety.
func (c *TXNBranch) ProposeFCFFNotSafe(msg *network.CoordinatorGossip, sent time.Time) *detector.KvRes {
	if !c.GetVote() {
		// if vote to abort, it will abort regardless of the ACP used.
		configs.TPrintf("TXN" + strconv.FormatUint(c.TID, 10) + ": " + c.from.stmt.address + " abort for PreWrite")
		c.Res.SetSelfResult(false, false, true)
		c.from.broadCastVote(msg, No, msg.ShardID, msg.ParticipantAddresses, sent)
		configs.Assert(c.from.Abort(msg), "impossible case, abort should not fail with all locks")
	} else {
		configs.DPrintf("TXN" + strconv.FormatUint(msg.TxnID, 10) + ": " + "failure-free level  " + c.from.stmt.address)
		hd := c.from.createIfNotExistMsgPool(msg.ShardID, msg.TxnID, len(msg.ParticipantAddresses))
		rem := c.from.stmt.GetNetworkTimeOut(msg.ParticipantAddresses) - time.Since(sent)
		configs.DPrintf("TXN" + strconv.FormatUint(msg.TxnID, 10) + ": " + " participant side timeout = " + rem.String())
		select {
		case <-time.After(rem): // No vote not received within the timeout window, thus directly pre-commit the batch.
			configs.TPrintf("TXN" + strconv.FormatUint(c.TID, 10) + ": " + c.from.stmt.address + " auto commit")
			c.CommitInEpoch(msg.EpochNum)
			c.Res.SetSelfResult(true, true, false)
			return c.Res
		case <-c.from.stmt.ctx.Done():
			configs.TPrintf("TXN" + strconv.FormatUint(c.TID, 10) + ": " + c.from.stmt.address + " abort for ctx done")
			c.Res.SetSelfResult(true, false, false)
			c.Abort()
			return c.Res
		case <-hd.finish:
			// No vote received, abort the transaction.
			c.Abort()
			c.Res.SetSelfResult(true, false, true)
			return c.Res
		}
	}
	return c.Res
}

// PreWrite execute write operations and persist redo logs.
func (c *TXNBranch) PreWrite() bool {
	defer configs.TimeTrack(time.Now(), "PreWrite", c.TID)
	c.Vote = c.GetVote()
	return c.Vote
}

func (c *TXNBranch) PreRead() (map[string]*storage.RowData, bool) {
	result := make(map[string]*storage.RowData)
	for _, v := range c.OptList {
		switch v.Type {
		case storage.ReadOpt:
			val, ok := c.Kv.ReadTxn(v.Table, uint32(c.TID), v.Key)
			if !ok {
				return nil, false
			} else {
				result[v.GetKey()] = val
			}
		default:
			panic(fmt.Errorf("no update operation shall be passed during pre-read"))
		}
	}
	return result, true
}

func (c *TXNBranch) CommitInEpoch(epochNum uint64) bool {
	defer configs.TimeTrack(time.Now(), "Commit", c.TID)
	return c.Kv.Commit(uint32(c.TID))
}

func (c *TXNBranch) AbortInEpoch(epochNum uint64) bool {
	defer configs.TimeTrack(time.Now(), "Commit", c.TID)
	return c.Kv.Commit(uint32(c.TID))
}

func (c *TXNBranch) PersistEpoch(epochNum uint64) bool {
	defer configs.TimeTrack(time.Now(), "Commit", c.TID)
	return c.Kv.Commit(uint32(c.TID))
}

func (c *TXNBranch) Commit() bool {
	defer configs.TimeTrack(time.Now(), "Commit", c.TID)
	return c.Kv.Commit(uint32(c.TID))
}

func (c *TXNBranch) Abort() bool {
	defer configs.TimeTrack(time.Now(), "Abort", c.TID)
	return c.Kv.RollBack(uint32(c.TID))
}

// GetVote checks if we can get all the locks to ensure ACID for write operations.
// tx should only contain write operations.
func (c *TXNBranch) GetVote() bool {
	for _, v := range c.OptList {
		if v.Type == storage.UpdateOpt {
			ok := c.Kv.UpdateTxn(v.Table, uint32(c.TID), v.Key, v.Value)
			if !ok {
				return false
			}
		} else {
			if !configs.StoredProcedure {
				continue // skip the read operations regarding the pre-read + getvote.
			}
			if v.Type == storage.ReadOpt {
				_, ok := c.Kv.ReadTxn(v.Table, uint32(c.TID), v.Key)
				if !ok {
					return false
				}
			} else {
				panic(fmt.Errorf("invalid operation"))
			}
		}
	}
	return c.Kv.Prepare(uint32(c.TID))
}
