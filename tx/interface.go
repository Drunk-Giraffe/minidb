// 为transaction定义接口
package tx

import (
	fm "file_manager"
)

// 定义一个接口
type TransactionInterface interface {
	Commit()
	Rollback()
	Recover()
	Pin(blk *fm.BlockID)
	Unpin(blk *fm.BlockID)
	GetInt(blk *fm.BlockID, offset uint64) uint64
	GetString(blk *fm.BlockID, offset uint64) string
	SetInt(blk *fm.BlockID, offset uint64, value uint64, shouldLog bool)
	SetString(blk *fm.BlockID, offset uint64, value string, shouldLog bool)
	AvailableBuffers() uint64
	Size(filename string) uint64
	Append(filename string) *fm.BlockID
	BlockSize() uint64
}

type RECORD_TYPE uint64

const (
	CHECKPOINT RECORD_TYPE = iota
	START
	COMMIT
	ROLLBACK
	SET_INT
	SET_STRING
)

type LogRecordInterface interface {
	Op() RECORD_TYPE
	TxID() uint64
	Undo(tx TransactionInterface)
	ToString() string
}
