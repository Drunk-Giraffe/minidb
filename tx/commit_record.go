package tx

import (
	fm "file_manager"
	"fmt"
	lm "log_manager"
)

type CommitRecord struct {
	txID uint64
}

func NewCommitRecord(p *fm.Page) *CommitRecord {
	txID := p.GetInt(uint64(8))
	return &CommitRecord{
		txID: txID,
	}
}

func (c *CommitRecord) Op() RECORD_TYPE {
	return COMMIT
}

func (c *CommitRecord) TxID() uint64 {
	return c.txID
}

func (c *CommitRecord) ToString() string {
	return fmt.Sprintf("<COMMIT %d>", c.txID)
}

func WriteCommitLog(lgmr *lm.LogManager, txID uint64) (uint64, error) {

	rec := make([]byte, 16)

	p := fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(COMMIT))
	p.SetInt(8, txID)
	return lgmr.AppendLogRecord(rec)
}
