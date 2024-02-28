package file_manager

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func TestFileManager(t *testing.T) {
	fm, _ := NewFileManager("test", 256)
	blk := NewBlockID("test", 1)
	page1 := NewPageBySize(fm.block_Size)
	pos1 := uint64(0)
	s := "hello world"
	page1.SetString(pos1, s)
	size := page1.MaxLengthForString(s)
	pos2 := pos1 + size
	val := uint64(1234)
	page1.SetInt(pos2, val)

	fm.Write(blk, page1)

	page2 := NewPageBySize(fm.BlockSize())
	fm.Read(blk, page2)

	require.Equal(t, val, page2.GetInt(pos2))
	require.Equal(t, s, page2.GetString(pos1))
}
