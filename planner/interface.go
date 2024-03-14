package planner

import (
	"parser"
	"record_manager"
	"tx"
)

type Plan interface {
	Open() interface{}
	BlocksAccessed() int
	RecordsOutput() int
	DistinctValues(field_name string) int
	Schema() record_manager.SchemaInterface
}

type QueryPlanner interface {
	CreatePlan(data *parser.Querydata, tx *tx.Transaction) Plan
}

type UpdatePlanner interface {
	ExecuteInsert(data *parser.InsertData, tx *tx.Transaction) int

	/*
		解释执行 delete 语句，返回被删除的记录数
	*/
	ExecuteDelete(data *parser.DeleteData, tx *tx.Transaction) int

	/*
		解释执行 create table 语句，返回新建表中的记录数
	*/
	ExecuteCreateTable(data *parser.CreateTableData, tx *tx.Transaction) int

	ExecuteModify(data *parser.ModifyData, tx *tx.Transaction) int
	/*
		解释执行 create index 语句，返回当前建立了索引的记录数
		TODO
	*/
	//ExecuteCreateIndex(data *parser.CreateIndexData, tx *tx.Transation) int
}
