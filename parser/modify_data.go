package parser

import "query"

type ModifyData struct {
	tableName string
	fieldName string
	newValue  *query.Expression
	pred      *query.Predicate
}

func NewModifyData(tableName, fieldName string, newValue *query.Expression, pred *query.Predicate) *ModifyData {
	return &ModifyData{tableName, fieldName, newValue, pred}
}

func (md *ModifyData) TableName() string {
	return md.tableName
}

func (md *ModifyData) FieldName() string {
	return md.fieldName
}

func (md *ModifyData) NewValue() *query.Expression {
	return md.newValue
}

func (md *ModifyData) Predicate() *query.Predicate {
	return md.pred
}
