package tx

import (
	bm "buffer_manager"
	fm "file_manager"
)

type BufferList struct {
	buffers    map[*fm.BlockID]*bm.Buffer
	buffer_mgr *bm.BufferManager
	pins       []*fm.BlockID
}

func NewBufferList(buffer_mgr *bm.BufferManager) *BufferList {
	buffer_list := &BufferList{
		buffer_mgr: buffer_mgr,
		buffers:    make(map[*fm.BlockID]*bm.Buffer),
		pins:       make([]*fm.BlockID, 0),
	}
	return buffer_list
}

func (bl *BufferList) GetBuffer(block_id *fm.BlockID) *bm.Buffer {
	buffer, _ := bl.buffers[block_id]
	return buffer
}

func (bl *BufferList) Pin(block_id *fm.BlockID) error {
	//如果给定的内存块被Pin了，那么把它加入到map中
	buffer, err := bl.buffer_mgr.Pin(block_id)
	if err != nil {
		return err
	}
	bl.buffers[block_id] = buffer
	bl.pins = append(bl.pins, block_id)
	return nil
}

func (bl *BufferList) Unpin(block_id *fm.BlockID) {
	//如果给定的内存块被Unpin了，那么把它从map中删除
	buffer, ok := bl.buffers[block_id]
	if !ok {
		return
	}
	bl.buffer_mgr.Unpin(buffer)
	for i, id := range bl.pins {
		if id == block_id {
			bl.pins = append(bl.pins[:i], bl.pins[i+1:]...)
			break
		}
	}
	delete(bl.buffers, block_id)
}

func (bl *BufferList) UnpinAll() {
	for _, id := range bl.pins {
		buffer, ok := bl.buffers[id]
		if ok {
			bl.buffer_mgr.Unpin(buffer)
		}
	}
	bl.pins = make([]*fm.BlockID, 0)
	bl.buffers = make(map[*fm.BlockID]*bm.Buffer)
}
