package tx

import (
	bm "buffer_manager"
	fm "file_manager"
	"fmt"
	lm "log_manager"
	"sync"
)

var tx_id_mu sync.Mutex
var next_tx_id = int32(0)

func NextTxID() int32 {
	tx_id_mu.Lock()
	defer tx_id_mu.Unlock()
	next_tx_id++
	return next_tx_id
}

type Transaction struct {
	concur_mgr       *ConcurrencyManager
	recovery_manager *RecoveryManager
	file_manager     *fm.FileManager
	log_manager      *lm.LogManager
	buffer_manager   *bm.BufferManager
	my_buffers       *BufferList
	tx_id            int32
}

func NewTransaction(fm *fm.FileManager, lm *lm.LogManager, bm *bm.BufferManager) *Transaction {
	tx_id := NextTxID()
	tx := &Transaction{
		file_manager:   fm,
		log_manager:    lm,
		buffer_manager: bm,
		my_buffers:     NewBufferList(bm),
		tx_id:          tx_id,
	}

	//创建并发管理器
	tx.concur_mgr = NewConcurrencyManager()
	//创建恢复管理器
	tx.recovery_manager = NewRecoveryManager(bm, lm, tx, tx_id)
	return tx
}

func (tx *Transaction) Commit() {
	tx.concur_mgr.Release()
	//提交事务
	tx.recovery_manager.Commit()
	r := fmt.Sprintf("transaction %d commited", tx.tx_id)
	fmt.Println(r)

	//释放并发管理器

	tx.my_buffers.UnpinAll()
}

func (tx *Transaction) Rollback() {
	//释放并发管理器
	tx.concur_mgr.Release()
	//回滚事务
	tx.recovery_manager.Rollback()
	r := fmt.Sprintf("transaction %d rollbacked", tx.tx_id)
	fmt.Println(r)

	//释放并发管理器
	tx.my_buffers.UnpinAll()
}
func (tx *Transaction) Recover() {
	//系统启动时会在所有交易执行前执行该函数
	tx.buffer_manager.FlushAll(tx.tx_id)
	//调用回复管理器的recover接口
	tx.recovery_manager.Recover()
}

func (tx *Transaction) Pin(block_id *fm.BlockID) {
	//将指定的块分配给缓存
	tx.my_buffers.Pin(block_id)
}

func (tx *Transaction) Unpin(block_id *fm.BlockID) {
	//释放指定的块
	tx.my_buffers.Unpin(block_id)
}

func (tx *Transaction) buffer_not_exist(block_id *fm.BlockID) error {
	return fmt.Errorf("no buffer found for block %d with file %s", block_id.BlockNum(), block_id.FileName())
}

func (tx *Transaction) GetInt(block_id *fm.BlockID, offset uint64) (int64, error) {
	//调用并发管理器加shared锁
	err := tx.concur_mgr.Slock(block_id)
	if err != nil {
		return -1, err
	}
	buffer := tx.my_buffers.GetBuffer(block_id)
	if buffer == nil {
		return -1, tx.buffer_not_exist(block_id)
	}
	return buffer.Contents().GetInt(offset), nil
}

func (tx *Transaction) GetString(block_id *fm.BlockID, offset uint64) (string, error) {
	//调用并发管理器加shared锁
	err := tx.concur_mgr.Slock(block_id)
	if err != nil {
		return "", err
	}
	buffer := tx.my_buffers.GetBuffer(block_id)
	if buffer == nil {
		return "", tx.buffer_not_exist(block_id)
	}
	return buffer.Contents().GetString(offset), nil
}

func (tx *Transaction) SetInt(block_id *fm.BlockID, offset uint64, val int64, shouldLog bool) error {
	//调用并发管理器加exclusive锁
	fmt.Println(tx.concur_mgr.hasXlock(*block_id), block_id)
	err := tx.concur_mgr.Xlock(block_id)

	if err != nil {
		return err
	}

	buffer := tx.my_buffers.GetBuffer(block_id)

	if buffer == nil {
		return tx.buffer_not_exist(block_id)
	}
	var lsn uint64
	if shouldLog {
		lsn, err = tx.recovery_manager.SetInt(buffer, offset)
		if err != nil {
			return err
		}
	}
	p := buffer.Contents()
	p.SetInt(offset, val)
	buffer.SetModified(tx.tx_id, lsn)
	fmt.Println("set int success")
	return nil
}

func (tx *Transaction) SetString(block_id *fm.BlockID, offset uint64, val string, shouldLog bool) error {
	//调用并发管理器加exclusive锁
	err := tx.concur_mgr.Xlock(block_id)
	if err != nil {
		return err
	}
	buffer := tx.my_buffers.GetBuffer(block_id)
	if buffer == nil {
		return tx.buffer_not_exist(block_id)
	}
	var lsn uint64
	if shouldLog {
		lsn, err = tx.recovery_manager.SetString(buffer, offset)
		if err != nil {
			return err
		}
	}
	p := buffer.Contents()
	p.SetString(offset, val)
	buffer.SetModified(tx.tx_id, lsn)

	return nil
}

func (tx *Transaction) Size(file_name string) (uint64, error) {
	//调用并发管理器加shared锁
	dummy_blk := fm.NewBlockID(file_name, END_OF_FILE)
	err := tx.concur_mgr.Slock(dummy_blk)
	if err != nil {
		return 0, err
	}
	s, _ := tx.file_manager.Size(file_name)
	return s, nil
}

func (tx *Transaction) Append(file_name string) (*fm.BlockID, error) {
	//调用并发管理器加exclusive锁
	dummy_blk := fm.NewBlockID(file_name, END_OF_FILE)
	err := tx.concur_mgr.Xlock(dummy_blk)
	if err != nil {
		return nil, err
	}
	blk, err := tx.file_manager.Append(file_name)
	if err != nil {
		return nil, err
	}
	return &blk, nil
}

func (tx *Transaction) BlockSize() uint64 {
	return tx.file_manager.BlockSize()
}

func (tx *Transaction) AvailableBuffers() uint64 {
	return uint64(tx.buffer_manager.Available())
}
