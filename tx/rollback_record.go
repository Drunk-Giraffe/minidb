package tx

import (
	fm "file_manager"
	"fmt"
	lg "log_manager"
)

type RollbackRecord struct {
	txID uint64
}

func NewRollbackRecord(p *fm.Page) *RollbackRecord {
	return &RollbackRecord{
		txID: uint64(p.GetInt(uint64(8))),
	}
}

func (r *RollbackRecord) Op() RECORD_TYPE {
	return ROLLBACK
}

func (r *RollbackRecord) TxID() uint64 {
	return r.txID
}

func (r *RollbackRecord) Undo(tx TransactionInterface) {
	//它没有回滚操作
}

func (r *RollbackRecord) ToString() string {
	return fmt.Sprintf("<ROLLBACK %d>", r.txID)
}

func WriteRollbackLog(lgmr *lg.LogManager, txID uint64) (uint64, error) {
	rec := make([]byte, 2*uint64(8))
	p := fm.NewPageByBytes(rec)
	p.SetInt(0, int64(ROLLBACK))
	p.SetInt(uint64(8), int64(txID))

	return lgmr.AppendLogRecord(rec)
}
