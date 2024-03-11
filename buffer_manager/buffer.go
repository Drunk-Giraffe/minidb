package buffer_manager

import (
	fmgr "file_manager"
	lmgr "log_manager"
)

type Buffer struct {
	fm       *fmgr.FileManager
	lm       *lmgr.LogManager
	contents *fmgr.Page
	blk      *fmgr.BlockID
	pins     uint32 //锁定次数
	txnum    int32  //事务号
	lsn      uint64 //日志序列号
}

func NewBuffer(file_mgr *fmgr.FileManager, log_mgr *lmgr.LogManager) *Buffer {

	return &Buffer{
		fm:       file_mgr,
		lm:       log_mgr,
		txnum:    -1,
		lsn:      0,
		contents: fmgr.NewPageBySize(file_mgr.BlockSize()),
	}
}

func (b *Buffer) Contents() *fmgr.Page {
	return b.contents
}

func (b *Buffer) Block() *fmgr.BlockID {
	return b.blk
}

func (b *Buffer) SetModified(txnum int32, lsn uint64) {
	//如果上层组件修改了缓存数据，必须调用这个接口进行标记
	b.txnum = txnum
	if lsn > 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) IsPinned() bool {
	//返回当前缓存是否被锁定
	return b.pins > 0
}

func (b *Buffer) ModifyingTx() int32 {
	//返回当前修改缓存的交易号
	return b.txnum
}

func (b *Buffer) AssignToBlock(blk *fmgr.BlockID) {
	//将缓存分配给指定的块
	b.Flush() //确保当前缓存中的数据被写入磁盘
	b.blk = blk
	b.fm.Read(b.blk, b.Contents())
	b.pins = 0
}

func (b *Buffer) Flush() {
	if b.txnum >= 0 {
		b.lm.FlushByLSN(b.lsn)
		b.fm.Write(b.blk, b.contents)
		b.txnum = -1
	}
}

func (b *Buffer) Pin() {
	//锁定缓存
	b.pins++
}

func (b *Buffer) Unpin() {
	//解锁缓存
	b.pins--
}
