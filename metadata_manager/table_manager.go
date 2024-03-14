package metadata_manager

import (
	"query"
	rm "record_manager"
	"tx"
)

const (
	MAX_NAME_LENGTH = 16
)

type TableManager struct {
	fcatLayout *rm.Layout
	tcatLayout *rm.Layout
}

func NewTableManager(isNew bool, tx *tx.Transaction) *TableManager {

	tm := &TableManager{}

	tcatSchema := rm.NewSchema()
	tcatSchema.AddStringField("table_name", MAX_NAME_LENGTH)
	tcatSchema.AddIntField("slot_size")

	tm.tcatLayout = rm.NewLayoutWithSchema(tcatSchema)

	fcatSchema := rm.NewSchema()
	fcatSchema.AddStringField("table_name", MAX_NAME_LENGTH)
	fcatSchema.AddStringField("field_name", MAX_NAME_LENGTH)
	fcatSchema.AddIntField("type")
	fcatSchema.AddIntField("length")
	fcatSchema.AddIntField("offset")
	tm.fcatLayout = rm.NewLayoutWithSchema(fcatSchema)

	if isNew {
		tm.CreateTable("tblcat", tcatSchema, tx)
		tm.CreateTable("fldcat", fcatSchema, tx)
	}

	return tm
}

func (tm *TableManager) CreateTable(tableName string, schema *rm.Schema, tx *tx.Transaction) {
	layout := rm.NewLayoutWithSchema(schema)
	tcat := query.NewTableScan(tx, "tblcat", tm.tcatLayout)
	tcat.Insert()
	tcat.SetString("table_name", tableName)
	tcat.SetInt("slot_size", layout.SlotSize())
	tcat.Close()

	fcat := query.NewTableScan(tx, "fldcat", tm.fcatLayout)
	for _, field_name := range schema.Fields() {
		fcat.Insert()
		fcat.SetString("table_name", tableName)
		fcat.SetString("field_name", field_name)
		fcat.SetInt("type", int(schema.Type(field_name)))
		fcat.SetInt("length", schema.Length(field_name))
		fcat.SetInt("offset", layout.Offset(field_name))
	}
	fcat.Close()
}

func (tm *TableManager) GetTableLayout(tableName string, tx *tx.Transaction) *rm.Layout {
	size := -1
	tcat := query.NewTableScan(tx, "tblcat", tm.tcatLayout)
	for tcat.Next() {
		if tcat.GetString("table_name") == tableName {
			size = tcat.GetInt("slot_size")
			break
		}
	}
	tcat.Close()

	sch := rm.NewSchema()
	offsets := make(map[string]int)
	fcat := query.NewTableScan(tx, "fldcat", tm.fcatLayout)
	for fcat.Next() {
		if fcat.GetString("table_name") == tableName {
			field_name := fcat.GetString("field_name")
			offset := fcat.GetInt("offset")
			field_type := fcat.GetInt("type")
			field_length := fcat.GetInt("length")
			offsets[field_name] = offset
			sch.AddField(field_name, rm.FIELD_TYPE(field_type), field_length)
		}
	}

	fcat.Close()
	return rm.NewLayout(sch, offsets, size)
}
