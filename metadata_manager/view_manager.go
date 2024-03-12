package metadata_manager

import (
	rm "record_manager"
	"tx"
)

const (
	//用于创建view的sql语句最大长度
	MAX_VIEW_DEF = 100
)

type ViewManager struct {
	tbl_mgr *TableManager
}

func NewViewManager(isNew bool, tbl_mgr *TableManager, tx *tx.Transaction) *ViewManager {
	vm := &ViewManager{tbl_mgr : tbl_mgr}

	if isNew {
		sch := rm.NewSchema()
		sch.AddStringField("viewname", MAX_NAME_LENGTH)
		sch.AddStringField("viewdef", MAX_VIEW_DEF)
		tbl_mgr.CreateTable("viewcat", sch, tx)
	}

	return vm

}

func (vm *ViewManager) CreateView(viewname string, viewdef string, tx *tx.Transaction) {
	layout := vm.tbl_mgr.GetTableLayout("viewcat", tx)
	ts := rm.NewTableScan(tx , "viewcat", layout)
	ts.Insert()
	ts.SetString("viewname", viewname)
	ts.SetString("viewdef", viewdef)
	ts.Close()
}

func (vm *ViewManager) GetViewDef(viewname string, tx *tx.Transaction) string {
	layout := vm.tbl_mgr.GetTableLayout("viewcat", tx)
	ts := rm.NewTableScan(tx, "viewcat", layout)
	for ts.Next() {
		if ts.GetString("viewname") == viewname {
			viewdef := ts.GetString("viewdef")
			ts.Close()
			return viewdef
		}
	}
	ts.Close()
	return ""
}