package buffer_manager

import (
	"errors"
	fmgr "file_manager"
	lmgr "log_manager"
	"sync"
	"time"
)

const (
	MAX_TIME_WAIT = 3 //分配缓存最大等待时间
)

type BufferManager struct {
	buffer_pool      []*Buffer
	buffer_available uint32
	mu               sync.Mutex
}

func NewBufferManager(fm *fmgr.FileManager, lm *lmgr.LogManager, num_buffers uint32) *BufferManager {
	bm := &BufferManager{
		buffer_available: num_buffers, //缓存池大小
	}
	for i := uint32(0); i < bm.buffer_available; i++ {
		buffer := NewBuffer(fm, lm)
		bm.buffer_pool = append(bm.buffer_pool, buffer)
	}
	return bm
}

func (bm *BufferManager) Available() uint32 {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	return bm.buffer_available
}

func (bm *BufferManager) FlushAll(txnum int32) {
	//将给定的事务号的数据修改写入磁盘
	bm.mu.Lock()
	defer bm.mu.Unlock()

	for _, buffer := range bm.buffer_pool {
		if buffer.ModifyingTx() == txnum {
			buffer.Flush()
		}
	}
}

func (bm *BufferManager) Pin(blk *fmgr.BlockID) (*Buffer, error) {
	//将指定的块分配给缓存
	bm.mu.Lock()
	defer bm.mu.Unlock()

	start_time := time.Now()
	buffer := bm.tryPin(blk)
	for buffer == nil && !bm.waitTooLong(start_time) {
		time.Sleep(MAX_TIME_WAIT * time.Second)
		buffer = bm.tryPin(blk)
		if buffer == nil {
			return nil, errors.New("no buffer available, be careful for deadlock")
		}
	}

	return buffer, nil
}

//目前unpin不会触发异步写回磁盘，只是释放缓存
func (bm *BufferManager) Unpin(buffer *Buffer) {
	//释放缓存
	bm.mu.Lock()
	defer bm.mu.Unlock()
	if buffer == nil {
		return
	}
	buffer.Unpin()
	if !buffer.IsPinned() {
		bm.buffer_available++
		//notifyAll() //唤醒等待的线程
	}
}


func (bm *BufferManager) waitTooLong(start_time time.Time) bool {
	//判断是否等待时间过长
	return time.Since(start_time) >= MAX_TIME_WAIT*time.Second
}

func (bm *BufferManager) tryPin(blk *fmgr.BlockID) *Buffer {
	//判断给定block是否已经在缓存中
	buffer := bm.findExistingBuffer(blk)
	if buffer == nil {
		buffer = bm.chooseUnpinnedBuffer()
		if buffer == nil {
			return nil
		}
		buffer.AssignToBlock(blk)
	}

	if !buffer.IsPinned() {
		bm.buffer_available--
	}

	buffer.Pin()
	return buffer
}

func (bm *BufferManager) findExistingBuffer(blk *fmgr.BlockID) *Buffer {
	//判断给定block是否已经在缓存中
	for _, buffer := range bm.buffer_pool {
		block := buffer.Block()
		if  block != nil && block.Equal(blk){
			return buffer
		}
	}
	return nil
}

func (bm *BufferManager) chooseUnpinnedBuffer() *Buffer {
	//选择一个未锁定的缓存
	for _, buffer := range bm.buffer_pool {
		if !buffer.IsPinned() {
			return buffer
		}
	}
	return nil
}


