package planner

import (
	"query"
	"record_manager"
)

type ProjectPlanner struct {
	p      Plan
	schema *record_manager.Schema
}

func NewProjectPlanner(p Plan, fieldList []string) *ProjectPlanner {
	project_plan := ProjectPlanner{
		p:      p,
		schema: record_manager.NewSchema(),
	}

	for _, field := range fieldList {
		project_plan.schema.Add(field, project_plan.p.Schema())
	}

	return &project_plan
}

func (p *ProjectPlanner) Open() interface{} {
	s := p.p.Open()
	return query.NewProjectScan(s.(query.Scan), p.schema.Fields())
}

func (p *ProjectPlanner) BlocksAccessed() int {
	return p.p.BlocksAccessed()
}

func (p *ProjectPlanner) RecordsOutput() int {
	return p.p.RecordsOutput()
}

func (p *ProjectPlanner) DistinctValues(fldName string) int {
	return p.p.DistinctValues(fldName)
}

func (p *ProjectPlanner) Schema() record_manager.SchemaInterface {
	return p.schema
}
