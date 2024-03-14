package planner

import (
	"metadata_manager"
	"query"
	"record_manager"
	"tx"
)

type TablePlanner struct {
	tx        *tx.Transaction
	tableName string
	layout    *record_manager.Layout
	si        *metadata_manager.StatInfo
}

func NewTablePlanner(tx *tx.Transaction, tableName string, md *metadata_manager.MetadataManager) *TablePlanner {
	tp := TablePlanner{tx: tx, tableName: tableName}
	tp.layout = md.GetLayout(tp.tableName, tp.tx)
	tp.si = md.GetStat(tp.tableName, tp.layout, tp.tx)
	return &tp
}

func (tp *TablePlanner) Open() interface{} {
	return query.NewTableScan(tp.tx, tp.tableName, tp.layout)
}

func (tp *TablePlanner) RecordsOutput() int {
	return tp.si.RecsOutput()
}

func (tp *TablePlanner) BlocksAccessed() int {
	return tp.si.BlocksAccessed()
}

func (tp *TablePlanner) DistinctValues(fieldName string) int {
	return tp.si.DistinctValues(fieldName)
}

func (tp *TablePlanner) Schema() record_manager.SchemaInterface {
	return tp.layout.Schema()
}
