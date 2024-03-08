package buffer_manager

import (
	fmgr "file_manager"
	lmgr "log_manager"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBufferManager(t *testing.T) {
	file_manager, _ := fmgr.NewFileManager("buffer_manager_test", 400)
	log_manager, _ := lmgr.NewLogManager(file_manager, "buffer_manager_test_log")
	buffer_manager := NewBufferManager(file_manager, log_manager, 3)

	buffer1, err := buffer_manager.Pin(fmgr.NewBlockID("test1", 1))
	require.Nil(t, err)

	p := buffer1.Contents()
	n := p.GetInt(80)
	p.SetInt(80, n+1)
	buffer1.SetModified(1, 0) //标记为修改

	buffer2, err2 := buffer_manager.Pin(fmgr.NewBlockID("test1", 2))
	require.Nil(t, err2)

	buffer3, err3 := buffer_manager.Pin(fmgr.NewBlockID("test1", 3))
	require.Nil(t, err3)

	_, err4 := buffer_manager.Pin(fmgr.NewBlockID("test1", 4))
	require.NotNil(t, err4) //缓存池已满，无法分配

	buffer1.Flush()
	buffer_manager.Unpin(buffer1)

	buffer_manager.Unpin(buffer2)
	buffer2, err = buffer_manager.Pin(fmgr.NewBlockID("test1", 1))
	require.Nil(t, err)

	p2 := buffer2.Contents()
	p2.SetInt(80, 9999)
	buffer2.SetModified(1, 0)     //标记为修改
	buffer_manager.Unpin(buffer2) //buffer2没有写入磁盘

	//读入test1的第一个块，确认buffer1的修改已经写入磁盘
	page := fmgr.NewPageBySize(400)
	blk1 := fmgr.NewBlockID("test1", 1)
	file_manager.Read(blk1, page)
	n1 := page.GetInt(80)
	require.Equal(t, n1, n+1)

	buffer3.Unpin()

}
