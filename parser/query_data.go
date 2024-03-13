package parser

//Querydata 用来描述select语句的操作信息
import (
	"query"
)

type Querydata struct {
	fields []string
	tables []string
	pred   *query.Predicate
}

func NewQuerydata(fields []string, tables []string, pred *query.Predicate) *Querydata {
	return &Querydata{
		fields: fields,
		tables: tables,
		pred:   pred,
	}
}

func (q *Querydata) Fields() []string {
	return q.fields
}

func (q *Querydata) Tables() []string {
	return q.tables
}

func (q *Querydata) Pred() *query.Predicate {
	return q.pred
}

func (q *Querydata) ToString() string {
	result := "select "
	for _, fldName := range q.fields {
		result += fldName + ", "
	}

	// 去掉最后一个逗号
	result = result[:len(result)-1]
	result += " from "
	for _, tableName := range q.tables {
		result += tableName + ", "
	}
	// 去掉最后一个逗号
	result = result[:len(result)-1]
	predStr := q.pred.ToString()
	if predStr != "" {
		result += " where " + predStr
	}

	return result
}
