package tx

import (
	bm "buffer_manager"
	fm "file_manager"
	"fmt"
	lm "log_manager"
	"testing"
	"time"
)

func TestConcurrencyManager(_ *testing.T) {
	/*
		创建三个线程，每个线程对应一个交易，这些交易读写相同的区块，判断区块读写时加锁逻辑是否正确
	*/
	//创建文件管理器
	fmgr, _ := fm.NewFileManager("txtest", 400)
	//创建日志管理器
	lmgr, _ := lm.NewLogManager(fmgr, "logfile")
	//创建缓冲区管理器
	bmgr := bm.NewBufferManager(fmgr, lmgr, 3)
	//创建交易
	go func() {
		txA := NewTransaction(fmgr, lmgr, bmgr)
		blk1 := fm.NewBlockID("testfile", 1)
		blk2 := fm.NewBlockID("testfile", 2)
		txA.Pin(blk1)
		txA.Pin(blk2)
		fmt.Println("Tx A request Slock 1")
		txA.GetInt(blk1, 0)
		fmt.Println("Tx A receive Slock 1")
		time.Sleep(2 * time.Second)

		fmt.Println("Tx A request Slock 2")
		txA.GetInt(blk2, 0)
		fmt.Println("Tx A receive Slock 2")
		fmt.Println("Tx A commit")
		txA.Commit()

	}()
	go func() {
		time.Sleep(1 * time.Second)
		txB := NewTransaction(fmgr, lmgr, bmgr)
		blk1 := fm.NewBlockID("testfile", 1)
		blk2 := fm.NewBlockID("testfile", 2)
		txB.Pin(blk1)
		txB.Pin(blk2)
		fmt.Println("Tx B request Xlock 2")
		txB.SetInt(blk2, 0, 0, false)

		fmt.Println("Tx B receive Xlock 2")
		time.Sleep(2 * time.Second)

		fmt.Println("Tx B request Slock 1")
		txB.GetInt(blk1, 0)
		fmt.Println("Tx B receive Slock 1")
		fmt.Println("Tx B commit")
		txB.Commit()

	}()
	go func() {
		time.Sleep(2 * time.Second)
		txC := NewTransaction(fmgr, lmgr, bmgr)
		blk1 := fm.NewBlockID("testfile", 1)
		blk2 := fm.NewBlockID("testfile", 2)
		txC.Pin(blk1)
		txC.Pin(blk2)
		fmt.Println("Tx C request Xlock 1")
		txC.SetInt(blk1, 0, 0, false)
		fmt.Println("Tx C receive Xlock 1")
		time.Sleep(1 * time.Second)

		fmt.Println("Tx C request Slock 2")
		txC.GetInt(blk2, 0)
		fmt.Println("Tx C receive Slock 2")
		fmt.Println("Tx C commit")
		txC.Commit()

	}()
	time.Sleep(20 * time.Second)
}
