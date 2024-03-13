package parser

import "record_manager"

type CreateTableData struct {
	tableName string
	schema    *record_manager.Schema
}

func NewCreateTableData(tableName string, schema *record_manager.Schema) *CreateTableData {
	return &CreateTableData{
		tableName: tableName,
		schema:    schema,
	}
}

func (ctd *CreateTableData) TableName() string {
	return ctd.tableName
}

func (ctd *CreateTableData) NewSchema() *record_manager.Schema {
	return ctd.schema
}
