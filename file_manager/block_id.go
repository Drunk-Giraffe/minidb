package file_manager

import (
	"crypto/sha256"
	"fmt"
)

type BlockID struct {
	file_name string // 对应磁盘上的文件名
	block_num uint64 // 文件的第几个块
}

func NewBlockID(file_name string, block_num uint64) *BlockID {
	return &BlockID{
		file_name: file_name,
		block_num: block_num,
	}
}

func (b *BlockID) FileName() string {
	return b.file_name
}

func (b *BlockID) BlockNum() uint64 {
	return b.block_num
}

func (b *BlockID) Equal(other *BlockID) bool {
	return b.file_name == other.file_name && b.block_num == other.block_num
}

func asSha256(o interface{}) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%v", o)))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (b *BlockID) HashCode() string {
	return asSha256(b)
}
