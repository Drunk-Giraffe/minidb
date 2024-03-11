package tx

import (
	fm "file_manager"
)

type ConcurrencyManager struct {
	lock_table *LockTable //单例模式
	lock_map   map[fm.BlockID]string
}

func NewConcurrencyManager() *ConcurrencyManager {
	concur_mgr := &ConcurrencyManager{
		lock_table: GetLockTableInstance(),
		lock_map:   make(map[fm.BlockID]string),
	}
	return concur_mgr
}

func (cm *ConcurrencyManager) Slock(blk *fm.BlockID) error {
	_, ok := cm.lock_map[*blk]
	if !ok {
		err := cm.lock_table.Slock(blk)
		if err != nil {
			return err
		}
		cm.lock_map[*blk] = "s"
	}
	return nil
}

func (cm *ConcurrencyManager) Xlock(blk *fm.BlockID) error {
	// _, ok := cm.lock_map[*blk]
	if !cm.hasXlock(*blk) {
		cm.Slock(blk)
		err := cm.lock_table.Xlock(blk)
		if err != nil {
			return err
		}
		cm.lock_map[*blk] = "x"
	}
	return nil
}

func (cm *ConcurrencyManager) Release() {
	for blk := range cm.lock_map {
		cm.lock_table.Unlock(&blk)
	}
}

func (cm *ConcurrencyManager) hasXlock(blk fm.BlockID) bool {
	lock_type, ok := cm.lock_map[blk]
	if ok && lock_type == "x" {
		return true
	}
	return false
}
