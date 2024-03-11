// 为transaction定义接口
package tx

import (
	fm "file_manager"
	"math"
)

// 定义一个接口
type TransactionInterface interface {
	Commit()
	Rollback()
	Recover()
	Pin(blk *fm.BlockID)
	Unpin(blk *fm.BlockID)
	GetInt(blk *fm.BlockID, offset uint64) (int64, error)
	GetString(blk *fm.BlockID, offset uint64) (string, error)
	SetInt(blk *fm.BlockID, offset uint64, value int64, shouldLog bool) error
	SetString(blk *fm.BlockID, offset uint64, value string, shouldLog bool) error
	AvailableBuffers() uint64
	Size(filename string) (uint64, error)
	Append(filename string) (*fm.BlockID, error)
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

const (
	END_OF_FILE = math.MaxUint64
)

type LogRecordInterface interface {
	Op() RECORD_TYPE
	TxID() uint64
	Undo(tx TransactionInterface)
	ToString() string
}
