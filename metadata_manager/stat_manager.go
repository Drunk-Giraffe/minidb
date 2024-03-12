package metadata_manager

import (
	rm "record_manager"
	"sync"
	"tx"
)

const (
	//数据库一百次操作后更新统计数据
	REFRESH_STAT_INFO_CONUT = 100
)

type StatInfo struct {
	numBlocks int //记录数据库使用了多少块
	numRecs   int //记录数据库有多少条记录
}

func newStatInfo(numBlocks int, numRecs int) *StatInfo {
	return &StatInfo{
		numBlocks: numBlocks,
		numRecs:   numRecs,
	}
}

func (s *StatInfo) BlocksAccessed() int {
	return s.numBlocks
}

func (s *StatInfo) RecsOutput() int {
	return s.numRecs
}

func (s *StatInfo) DistinctValues(field_name string) int {
	return 1 * (s.numRecs / 3) //假设每个字段有三个不同的值
}

type StatManager struct {
	tblMgr     *TableManager
	tableStats map[string]*StatInfo
	numCalls   int
	lock       sync.Mutex
}

func NewStatManager(tblMgr *TableManager, tx *tx.Transaction) *StatManager {
	statMgr := &StatManager{
		tblMgr:   tblMgr,
		numCalls: 0,
	}

	statMgr.refreshStatistics(tx)
	return statMgr
}

func (sm *StatManager) GetStatInfo(tblName string, layout *rm.Layout, tx *tx.Transaction) *StatInfo {
	sm.lock.Lock()
	defer sm.lock.Unlock()

	sm.numCalls++
	if sm.numCalls > REFRESH_STAT_INFO_CONUT {
		sm.refreshStatistics(tx)
	}

	si := sm.tableStats[tblName]
	if si == nil {
		si = sm.calTableStats(tblName, layout, tx)
		sm.tableStats[tblName] = si
	}
	return si
}

func (sm *StatManager) refreshStatistics(tx *tx.Transaction) {
	sm.tableStats = make(map[string]*StatInfo)
	sm.numCalls = 0
	tcatLayout := sm.tblMgr.GetTableLayout("tblcat", tx)
	tcat := rm.NewTableScan(tx, "tblcat", tcatLayout)
	for tcat.Next() {
		tblName := tcat.GetString("tblname")
		layout := sm.tblMgr.GetTableLayout(tblName, tx)
		sm.tableStats[tblName] = sm.calTableStats(tblName, layout, tx)
	}
	tcat.Close()
}

func (sm *StatManager) calTableStats(tblName string, layout *rm.Layout, tx *tx.Transaction) *StatInfo {
	numRecs := 0
	numBlocks := 0
	ts := rm.NewTableScan(tx, tblName, layout)
	for ts.Next() {
		numRecs++
		numBlocks = ts.GetRid().BlockID() + 1
	}
	ts.Close()
	return newStatInfo(numBlocks, numRecs)
}
