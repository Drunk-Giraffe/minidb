package tx

import (
	fm "file_manager"
	"fmt"
	lm "log_manager"
)

type StartRecord struct {
	txID      uint64
	log_manager *lm.LogManager
}

func NewStartRecord(p *fm.Page, log_manager *lm.LogManager) *StartRecord {
	//开头的8个字节是日志类型，接下来的8个字节是事务编号
	txID := p.GetInt(8)
	return &StartRecord{
		txID:      uint64(txID),
		log_manager: log_manager,
	}
}

func (sr *StartRecord) Op() RECORD_TYPE {
	return START
}

func (sr *StartRecord) TxID() uint64 {
	return sr.txID
}

func (sr *StartRecord) Undo(tx TransactionInterface) {
	//不需要做任何事情
}

func (sr *StartRecord) ToString() string {
	return fmt.Sprintf("<START %d>", sr.txID)
}

func (sr *StartRecord) WriteStartLog() (uint64, error) {
	record := make([]byte, 16)
	p := fm.NewPageByBytes(record)
	p.SetInt(uint64(0), int64(START))
	p.SetInt(uint64(8), int64(sr.txID))
	return sr.log_manager.AppendLogRecord(record)
}
