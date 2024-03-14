package parser

import "fmt"

type IndexData struct {
	indexName string
	tableName string
	fieldName string
}

func NewIndexData(indexName, tableName, fieldName string) *IndexData {
	return &IndexData{indexName, tableName, fieldName}
}

func (id *IndexData) IndexName() string {
	return id.indexName
}

func (id *IndexData) TableName() string {
	return id.tableName
}

func (id *IndexData) FieldName() string {
	return id.fieldName
}

func (id *IndexData) ToString() string {
	return fmt.Sprintf("Index: %s\nTable: %s\nField: %s", id.indexName, id.tableName, id.fieldName)
}
