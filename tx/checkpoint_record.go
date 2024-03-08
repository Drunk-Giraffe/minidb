package tx

import (
	fm "file_manager"
	lg "log_manager"
	"math"
)

type CheckPointRecord struct {
}

func NewCheckPointRecord() *CheckPointRecord {
	return &CheckPointRecord{}
}

func (c *CheckPointRecord) Op() RECORD_TYPE {
	return CHECKPOINT
}

func (c *CheckPointRecord) TxID() uint64 {
	return math.MaxUint64 //它没有对应的交易号
}

func (c *CheckPointRecord) Undo(tx TransactionInterface) {

}

func (c *CheckPointRecord) ToString() string {
	return "<CHECKPOINT>"
}

func WriteCheckPointLog(lgmr *lg.LogManager) (uint64, error) {
	rec := make([]byte, uint64(8))
	p := fm.NewPageByBytes(rec)
	p.SetInt(0, int64(CHECKPOINT))
	return lgmr.AppendLogRecord(rec)
}
