package planner

import (
	"metadata_manager"
	"parser"
	"tx"
)

type BasicQueryPlanner struct {
	mm *metadata_manager.MetadataManager
}

func NewBasicQueryPlanner(mm *metadata_manager.MetadataManager) *BasicQueryPlanner {
	return &BasicQueryPlanner{mm: mm}
}

func (qp *BasicQueryPlanner) CreatePlan(data *parser.Querydata, tx *tx.Transaction) Plan {
	plans := make([]Plan, 0)
	tables := data.Tables()
	for _, table := range tables {
		viewDef := qp.mm.GetViewDef(table, tx)
		if viewDef != "" {
			parser := parser.NewSQLParser(viewDef)
			viewData := parser.Query()
			plans = append(plans, qp.CreatePlan(viewData, tx))

		} else {
			plans = append(plans, NewTablePlanner(tx, table, qp.mm))
		}
	}

	plan := plans[0]
	plans = plans[1:]
	for _, nextPlan := range plans {
		plan = NewProductPlanner(plan, nextPlan)
	}
	p := NewSelectPlanner(plan, data.Pred())

	return NewProjectPlanner(p, data.Fields())
}
