package tx

import (
	bm "buffer_manager"
	fm "file_manager"
	lm "log_manager"
)

// RecoveryManager is responsible for recovering the database after a crash.

type RecoveryManager struct {
	buffer_manager *bm.BufferManager
	log_manager    *lm.LogManager
	tx             *Transaction
	txID           int32
}

func NewRecoveryManager(buffer_manager *bm.BufferManager, log_manager *lm.LogManager,
	tx *Transaction, txID int32) *RecoveryManager {
	recover_mgr := &RecoveryManager{
		buffer_manager: buffer_manager,
		log_manager:    log_manager,
		tx:             tx,
		txID:           txID,
	}

	//产生一条StartLog
	p := fm.NewPageBySize(32)
	p.SetInt(0, int64(START))
	p.SetInt(8, int64(txID))
	start_record := NewStartRecord(p, log_manager)
	start_record.WriteStartLog()

	return recover_mgr
}

func (rm *RecoveryManager) Commit() error {
	rm.buffer_manager.FlushAll(rm.txID)
	lsn, err := WriteCommitLog(rm.log_manager, uint64(rm.txID))

	if err != nil {
		return err
	}

	rm.log_manager.FlushByLSN(lsn)

	return nil
}

func (rm *RecoveryManager) Rollback() error {
	rm.doRollback()

	rm.buffer_manager.FlushAll(rm.txID)
	lsn, err := WriteRollbackLog(rm.log_manager, uint64(rm.txID))
	if err != nil {
		return err
	}

	rm.log_manager.FlushByLSN(lsn)

	return nil
}

func (rm *RecoveryManager) Recover() error {
	rm.doRecover()
	rm.buffer_manager.FlushAll(rm.txID)
	lsn, err := WriteCheckPointLog(rm.log_manager)
	if err != nil {
		return err
	}

	rm.log_manager.FlushByLSN(lsn)

	return nil
}

func (rm *RecoveryManager) SetInt(buffer *bm.Buffer, offset uint64) (uint64, error) {
	old_val := buffer.Contents().GetInt(offset)
	blk := buffer.Block()
	return WriteSetIntLog(rm.log_manager, uint64(rm.txID), blk, offset, old_val)

}

func (rm *RecoveryManager) SetString(buffer *bm.Buffer, offset uint64) (uint64, error) {
	old_val := buffer.Contents().GetString(offset)
	blk := buffer.Block()
	return WriteSetStringLog(rm.log_manager, uint64(rm.txID), blk, offset, old_val)
}

func (rm *RecoveryManager) CreateRecord(bytes []byte) LogRecordInterface {
	p := fm.NewPageByBytes(bytes)
	switch RECORD_TYPE(p.GetInt(0)) {
	case CHECKPOINT:
		return NewCheckPointRecord()
	case START:
		return NewStartRecord(p, rm.log_manager)
	case COMMIT:
		return NewCommitRecord(p)
	case ROLLBACK:
		return NewRollbackRecord(p)
	case SET_INT:
		return NewSetIntRecord(p)
	case SET_STRING:
		return NewSetStringRecord(p)
	default:
		panic("Unknown record type")
	}
}

func (rm *RecoveryManager) doRollback() {
	//从日志文件中读取所有的记录
	iter := rm.log_manager.Iterator()
	for iter.HasNext() {
		rec := iter.Next()
		log_record := rm.CreateRecord(rec)
		if log_record.TxID() == uint64(rm.txID) {
			if log_record.Op() == START {
				return
			}
			log_record.Undo(rm.tx)
		}
	}
}


func (rm *RecoveryManager) doRecover() {
	finishedTxs := make(map[uint64]bool)
	//从日志文件中读取所有的记录
	iter := rm.log_manager.Iterator()
	for iter.HasNext() {
		rec := iter.Next()
		log_record := rm.CreateRecord(rec)
		// 标记完成的事务
		if log_record.Op() == COMMIT || log_record.Op() == ROLLBACK {
			finishedTxs[log_record.TxID()] = true
		}
	}
	iter = rm.log_manager.Iterator() // 重新开始遍历
	for iter.HasNext() {
		rec := iter.Next()
		log_record := rm.CreateRecord(rec)
		_, finished := finishedTxs[log_record.TxID()]
		// 如果事务未完成，则执行Undo
		if !finished {
			log_record.Undo(rm.tx)
		}
	}
}

