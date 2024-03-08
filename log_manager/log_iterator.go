package log_manager

import (
	fm "file_manager"
)

// LogIterator用于遍历日志文件，读取时倒序读取

type LogIterator struct {
	file_manager *fm.FileManager
	blk 		*fm.BlockID
	page 		*fm.Page
	current_pos uint64
	boundary 	uint64
}

func NewLogIterator(file_manager *fm.FileManager, blk *fm.BlockID) *LogIterator {
	it := LogIterator{
		file_manager: file_manager,
		blk: blk,
	}
	it.page = fm.NewPageBySize(file_manager.BlockSize())
	err :=it.moveToBlock(blk)
	if err != nil {
		return nil
	}
	return &it
}
func (it *LogIterator) moveToBlock(blk *fm.BlockID) error {
	_, err := it.file_manager.Read(blk, it.page)
	if err != nil {
		return err
	}
	it.boundary = uint64(it.page.GetInt(0))
	it.current_pos = it.boundary
	return nil
}

func (it *LogIterator) Next() []byte {
	// 若当前块已读完，读取上一个块
	if it.current_pos == it.file_manager.BlockSize() {
		it.blk = fm.NewBlockID(it.blk.FileName(), it.blk.BlockNum()-1)
		it.moveToBlock(it.blk)
	}

	// 读取日志
	record := it.page.GetBytes(it.current_pos )
	it.current_pos += 8 + uint64(len(record))
	return record
}

func (it *LogIterator) HasNext() bool {
	return it.blk.BlockNum() > 0 || it.current_pos < it.file_manager.BlockSize()
}