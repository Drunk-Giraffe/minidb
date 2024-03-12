package metadata_manager

import (
	rm "record_manager"
	"tx"
)

type MetadataManager struct {
	tblMgr  *TableManager
	viewMgr *ViewManager
	statMgr *StatManager
	//idxMgr *IndexManager
}

func NewMetadataManager(isNew bool, tx *tx.Transaction) *MetadataManager {
	mm := &MetadataManager{
		tblMgr: NewTableManager(isNew, tx),
	}
	mm.viewMgr = NewViewManager(isNew, mm.tblMgr, tx)
	mm.statMgr = NewStatManager(mm.tblMgr, tx)
	return mm
}

func (mm *MetadataManager) CreateTable(tblName string, sch *rm.Schema, tx *tx.Transaction) {
	mm.tblMgr.CreateTable(tblName, sch, tx)
}

func (mm *MetadataManager) CreateView(viewName string, viewDef string, tx *tx.Transaction) {
	mm.viewMgr.CreateView(viewName, viewDef, tx)
}

func (mm *MetadataManager) GetLayout(tblName string, tx *tx.Transaction) *rm.Layout {
	return mm.tblMgr.GetTableLayout(tblName, tx)
}

func (mm *MetadataManager) GetViewDef(viewName string, tx *tx.Transaction) string {
	return mm.viewMgr.GetViewDef(viewName, tx)
}

func (mm *MetadataManager) GetStat(tblName string, layout *rm.Layout, tx *tx.Transaction) *StatInfo {
	return mm.statMgr.GetStatInfo(tblName, layout, tx)
}
