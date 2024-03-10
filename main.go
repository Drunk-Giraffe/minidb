package main

import (
	bm "buffer_manager"
	fm "file_manager"
	"fmt"
	lm "log_manager"
	mm "metadata_manager"
	rm "record_manager"
	"tx"
)

func main() {
	field_manager, _ := fm.NewFileManager("record_manager_test", 400)
	log_manager, _ := lm.NewLogManager(field_manager, "record_manager_log")
	buffer_manager := bm.NewBufferManager(field_manager, log_manager, 3)

	tx := tx.NewTransaction(field_manager, log_manager, buffer_manager)
	sch := rm.NewSchema()

	sch.AddIntField("a")
	sch.AddStringField("b", 9)

	tm := mm.NewTableManager(true, tx)
	tm.CreateTable("test_table", sch, tx)
	layout := tm.GetTableLayout("test_table", tx)
	size := layout.LengthInBytes()
	sch2 := layout.Schema()
	fmt.Printf("size: %d\n", size)
	fmt.Printf("fields: ")
	for _, field := range sch2.Fields() {
		field_type := ""
		if sch2.Type(field) == rm.INTEGER {
			field_type = "INTEGER"
		} else {
			str_len := sch2.Length(field)
			field_type = fmt.Sprintf("VARCHAR(%d)", str_len)
		}
		fmt.Printf("%s %s, ", field, field_type)
	}
	fmt.Printf("\n")
	tx.Commit()
}
