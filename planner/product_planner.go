package planner

import (
	"query"
	"record_manager"
)

type ProductPlanner struct {
	p1     Plan
	p2     Plan
	schema *record_manager.Schema
}

func NewProductPlanner(p1 Plan, p2 Plan) *ProductPlanner {
	product_plan := ProductPlanner{
		p1:     p1,
		p2:     p2,
		schema: record_manager.NewSchema(),
	}

	product_plan.schema.AddAll(p1.Schema())
	product_plan.schema.AddAll(p2.Schema())
	return &product_plan
}

func (p *ProductPlanner) Open() interface{} {
	s1 := p.p1.Open()
	s2 := p.p2.Open()
	return query.NewProductScan(s1.(query.Scan), s2.(query.Scan))
}

func (p *ProductPlanner) BlocksAccessed() int {
	return p.p1.BlocksAccessed() + (p.p1.RecordsOutput() * p.p2.BlocksAccessed())
}

func (p *ProductPlanner) DistinctValues(fldName string) int {
	if p.p1.Schema().HasFields(fldName) {
		return p.p1.DistinctValues(fldName)
	} else {
		return p.p2.DistinctValues(fldName)
	}
}

func (p *ProductPlanner) RecordsOutput() int {
	return p.p1.RecordsOutput() * p.p2.RecordsOutput()
}

func (p *ProductPlanner) Schema() record_manager.SchemaInterface {
	return p.schema
}
