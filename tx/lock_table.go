package tx

import (
	"errors"
	fm "file_manager"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	MAX_WAITING_TIME = 10
)

type LockTable struct {
	lock_map    map[fm.BlockID]int64         //实现s锁和x锁，-1表示x锁，>0表示s锁
	notify_chan map[fm.BlockID]chan struct{} //通知挂起的所有线程恢复执行的信号
	//notifyAll,waitGivenTimeOut(blk)
	notify_wg   map[fm.BlockID]*sync.WaitGroup
	method_lock sync.Mutex
}

var lock_table_instance *LockTable
var lock = &sync.Mutex{}

func GetLockTableInstance() *LockTable {
	lock.Lock()
	defer lock.Unlock()
	if lock_table_instance == nil {
		lock_table_instance = NewLockTable()
	}
	return lock_table_instance
}

func (lt *LockTable) waitGivenTimeOut(blk *fm.BlockID) {
	wg, ok := lt.notify_wg[*blk]
	if !ok {
		var new_wg sync.WaitGroup
		lt.notify_wg[*blk] = &new_wg
		wg = &new_wg
	}
	wg.Add(1)
	defer wg.Done()
	lt.method_lock.Unlock()

	select {
	case <-time.After(MAX_WAITING_TIME * time.Second):
		fmt.Println("routine wake up for waiting timeout")
	case <-lt.notify_chan[*blk]:
		fmt.Println("routine wake up for notify signal")
	}
	lt.method_lock.Lock()
}

func (lt *LockTable) notifyAll(blk *fm.BlockID) {
	s := fmt.Sprintf("close channel for block %v\n", blk)
	fmt.Println(s)

	channel, ok := lt.notify_chan[*blk]
	if ok {
		close(channel)
		delete(lt.notify_chan, *blk)
		mark := rand.Intn(10000)

		s := fmt.Sprintf("delete blk: %v and launch rotinue to create it, mark: %d\n", *blk, mark)
		fmt.Println(s)

		go func(blk_unlock fm.BlockID, ran_num int) {
			s := fmt.Sprintf("wait group for blk: %v, with mark:%d\n", blk_unlock, ran_num)
			fmt.Print(s)
			lt.notify_wg[blk_unlock].Wait()
			//访问内部数据时需要加锁
			lt.method_lock.Lock()
			lt.notify_chan[blk_unlock] = make(chan struct{})
			lt.method_lock.Unlock()
			s = fmt.Sprintf("create notify channel for %v\n", blk_unlock)
			fmt.Print(s)

		}(*blk, mark)
	} else {
		s = fmt.Sprintf("channel for %v is already closed\n", *blk)
		fmt.Print(s)
	}

}

func NewLockTable() *LockTable {
	lock_table := &LockTable{
		lock_map:    make(map[fm.BlockID]int64),
		notify_chan: make(map[fm.BlockID]chan struct{}),
		notify_wg:   make(map[fm.BlockID]*sync.WaitGroup),
	}

	return lock_table
}

func (lt *LockTable) initWaitingOnBlock(blk *fm.BlockID) {
	_, ok := lt.notify_chan[*blk]
	if !ok {
		lt.notify_chan[*blk] = make(chan struct{})
	}

	_, ok = lt.notify_wg[*blk]
	if !ok {
		lt.notify_wg[*blk] = &sync.WaitGroup{}
	}

}

func (lt *LockTable) Slock(blk *fm.BlockID) error {
	lt.method_lock.Lock()
	defer lt.method_lock.Unlock()

	lt.initWaitingOnBlock(blk)

	start := time.Now()
	for lt.hasXlock(blk) && !lt.waitingTooLong(start) {
		lt.waitGivenTimeOut(blk)
	}

	if lt.hasXlock(blk) {
		fmt.Println("slock waiting too long")
		return errors.New("slock exception: xlock on given block")
	}

	val := lt.getLockValue(blk)
	lt.lock_map[*blk] = val + 1

	return nil

}

func (lt *LockTable) Xlock(blk *fm.BlockID) error {
	lt.method_lock.Lock()
	defer lt.method_lock.Unlock()

	lt.initWaitingOnBlock(blk)

	start := time.Now()
	for lt.hasOtherSlock(blk) && !lt.waitingTooLong(start) {
		fmt.Println("get xlock fail and sleep")
		lt.waitGivenTimeOut(blk)
	}

	if lt.hasOtherSlock(blk) {
		fmt.Println("xlock waiting too long")
		return errors.New("xlock exception: slock on given block")
	}

	lt.lock_map[*blk] = -1
	return nil
}

// 删除lock_map中的blk还是置为0？
func (lt *LockTable) Unlock(blk *fm.BlockID) {
	lt.method_lock.Lock()
	defer lt.method_lock.Unlock()

	val := lt.getLockValue(blk)
	if val > 1 {
		lt.lock_map[*blk] = val - 1
	} else {
		delete(lt.lock_map, *blk)
		//通知所有等待给定区块的线程从Wait中恢复
		s := fmt.Sprintf("unlock by blk: +%v\n", *blk)
		fmt.Println(s)
		lt.notifyAll(blk)
	}
}

func (lt *LockTable) hasXlock(blk *fm.BlockID) bool {
	return lt.getLockValue(blk) < 0
}

func (lt *LockTable) hasOtherSlock(blk *fm.BlockID) bool {
	return lt.getLockValue(blk) > 1
}

func (lt *LockTable) waitingTooLong(start time.Time) bool {
	return time.Since(start).Seconds() >= MAX_WAITING_TIME
}

func (lt *LockTable) getLockValue(blk *fm.BlockID) int64 {
	val, ok := lt.lock_map[*blk]
	if !ok {
		lt.lock_map[*blk] = 0
		val = 0
	}
	return val
}
