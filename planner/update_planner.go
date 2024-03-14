package planner

import (
	metadata_manager "metadata_manager"
	"parser"
	"query"
	"tx"
)

type BasicUpdatePlanner struct {
	mm *metadata_manager.MetadataManager
}

func NewBasicUpdatePlanner(mm *metadata_manager.MetadataManager) *BasicUpdatePlanner {
	return &BasicUpdatePlanner{mm: mm}
}

func (up *BasicUpdatePlanner) ExecuteInsert(data *parser.InsertData, tx *tx.Transaction) int {
	tablePlan := NewTablePlanner(tx, data.TableName(), up.mm)
	updateScan := tablePlan.Open().(*query.TableScan)
	updateScan.Insert()
	insertFields := data.Fields()
	insertValues := data.Vals()
	for i := 0; i < len(insertFields); i++ {
		updateScan.SetVal(insertFields[i], insertValues[i])
	}

	updateScan.Close()
	return 1
}

func (up *BasicUpdatePlanner) ExecuteDelete(data *parser.DeleteData, tx *tx.Transaction) int {
	tablePlan := NewTablePlanner(tx, data.TableName(), up.mm)
	selectPlan := NewSelectPlanner(tablePlan, data.Predicate())
	scan := selectPlan.Open()
	updateScan := scan.(*query.SelectScan)
	count := 0
	for updateScan.Next() {
		updateScan.Delete()
		count++
	}
	updateScan.Close()
	return count
}

func (up *BasicUpdatePlanner) ExecuteCreateTable(data *parser.CreateTableData, tx *tx.Transaction) int {
	up.mm.CreateTable(data.TableName(), data.NewSchema(), tx)
	return 0
}

// func (up *BasicUpdatePlanner) ExecuteCreateIndex(data *parser.CreateIndexData, tx *tx.Transaction) int {

// 	return 0
// }

func (up *BasicUpdatePlanner) ExecuteModify(data *parser.ModifyData, tx *tx.Transaction) int {
	tablePlan := NewTablePlanner(tx, data.TableName(), up.mm)
	selectPlan := NewSelectPlanner(tablePlan, data.Predicate())
	scan := selectPlan.Open()
	updateScan := scan.(*query.SelectScan)
	count := 0
	for updateScan.Next() {
		val := data.NewValue().Evaluate(scan.(query.Scan))
		updateScan.SetVal(data.FieldName(), val)
		count++
	}
	updateScan.Close()
	return count
}
