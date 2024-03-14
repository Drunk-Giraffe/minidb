package planner

import (
	"query"
	"record_manager"
)

type SelectPlanner struct {
	p    Plan
	pred *query.Predicate
}

func NewSelectPlanner(p Plan, pred *query.Predicate) *SelectPlanner {
	return &SelectPlanner{p: p, pred: pred}
}

func (sp *SelectPlanner) Open() interface{} {
	scan := sp.p.Open()
	return query.NewSelectScan(scan.(query.UpdateScan), sp.pred)
}

func (sp *SelectPlanner) BlocksAccessed() int {
	return sp.p.BlocksAccessed()
}

// 假设字段不同取值的数量为所有记录的1/3
func (sp *SelectPlanner) RecordsOutput() int {
	return sp.p.RecordsOutput() / sp.pred.ReductionFactor(sp.p)
}

func (sp *SelectPlanner) min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (sp *SelectPlanner) DistinctValues(field_name string) int {
	if sp.pred.EquatesWithConstant(field_name) != nil {
		return 1
	} else {
		fieldName2 := sp.pred.EquatesWithField(field_name)
		if fieldName2 != "" {
			return sp.min(sp.p.DistinctValues(field_name), sp.p.DistinctValues(fieldName2))
		} else {
			return sp.p.DistinctValues(field_name)
		}
	}
}

func (sp *SelectPlanner) Schema() record_manager.SchemaInterface {
	return sp.p.Schema()
}
