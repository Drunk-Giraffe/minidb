package main

import (
	"fmt"
	rm "record_manager"
)

func main() {
	// field_manager := fm.NewFileManager("record_manager_test", 400)
	// log_manager := lm.NewLogManager(field_manager)
	// buffer_manager := bm.NewBufferManager(field_manager, log_manager,3)

	// tx := tx.NewTransaction(field_manager, log_manager, buffer_manager)
	sch := rm.NewSchema()
	sch.AddIntField("A")
	sch.AddStringField("B", 100)
	sch.AddIntField("C")
	layout := rm.NewLayoutWithSchema(sch)
	for _, field_name := range sch.Fields() {
		offset := layout.Offset(field_name)
		fmt.Println("offset of ", field_name, " is ", offset)
	}
}
