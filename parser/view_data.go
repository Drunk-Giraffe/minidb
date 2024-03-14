package parser

import "fmt"

type ViewData struct {
	viewName  string
	queryData *Querydata
}

func NewViewData(viewName string, queryData *Querydata) *ViewData {
	return &ViewData{viewName, queryData}
}

func (vd *ViewData) ViewName() string {
	return vd.viewName
}

func (vd *ViewData) ViewDef() string {
	return vd.queryData.ToString()
}

func (vd *ViewData) ToString() string {
	return fmt.Sprintf("View: %s\n%s", vd.viewName, vd.queryData.ToString())
}
