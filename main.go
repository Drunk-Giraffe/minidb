package main

import (
	//"encoding/binary"
	fm "file_manager"

	"fmt"
	"sync"
	"time"
	"tx"
)

func main() {
	/*
		启动4个线程，第一个线程为区块1加x锁，然后启动剩下的三个线程为区块1加s锁，
		当后面的三个线程进入挂起状态时，第一个线程释放锁，然后后面的三个线程恢复执行，
		于是后面的三个线程都能够成功加s锁，获取区块1的数据
	*/
	blk := fm.NewBlockID("test", 1)
	var err_array []error
	var err_array_lock sync.Mutex
	lock_table := tx.NewLockTable()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		go func(i int) {
			fmt.Println("routine ", i, " start")
			wg.Add(1)
			defer wg.Done()
			err_array_lock.Lock()
			defer err_array_lock.Unlock()
			err := lock_table.Slock(blk)
			if err != nil {
				fmt.Println("routine ", i, " slock error")

			} else {
				fmt.Println("routine ", i, " slock success")
			}
			err_array = append(err_array, err)
		}(i)

	}

	time.Sleep(1 * time.Second) //让三个线程启动起来
	lock_table.Unlock(blk)
	start := time.Now()
	wg.Wait()

	elapsed := time.Since(start).Seconds()
	fmt.Println("elapsed time is ", elapsed, "\n")
}
