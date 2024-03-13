package query

import (
	fm "file_manager"
	"record_manager"
	"tx"
)

type TableScan struct {
	tx           *tx.Transaction
	layout       record_manager.LayoutInterface
	rm           record_manager.RecordManagerInterface
	file_name    string
	current_slot int
}

func NewTableScan(tx *tx.Transaction, table_name string, layout record_manager.LayoutInterface) *TableScan {
	ts := &TableScan{
		tx:        tx,
		layout:    layout,
		file_name: table_name + ".tbl",
	}

	size, err := tx.Size(ts.file_name)
	if err != nil {
		panic(err)
	}
	if size == 0 {
		ts.MoveToNewBlock()
	} else {
		ts.MoveToBlock(0)
	}

	return ts
}

func (ts *TableScan) Close() {
	if ts.rm != nil {
		ts.tx.Unpin(ts.rm.Block())
	}
}

func (ts *TableScan) PointBeforeFirst() {
	ts.MoveToBlock(0)
}

func (ts *TableScan) Next() bool {
	ts.current_slot = ts.rm.NextAfter(ts.current_slot)
	for ts.current_slot < 0 {
		if ts.AtLastBlock() {
			return false
		}

		ts.MoveToBlock(int(ts.rm.Block().BlockNum() + 1))
		ts.current_slot = ts.rm.NextAfter(ts.current_slot)
	}

	return true
}

func (ts *TableScan) GetInt(field_name string) int {
	return ts.rm.GetInt(ts.current_slot, field_name)
}

func (ts *TableScan) GetString(field_name string) string {
	return ts.rm.GetString(ts.current_slot, field_name)
}

func (ts *TableScan) GetVal(field_name string) *Constant {
	if ts.layout.Schema().Type(field_name) == record_manager.INTEGER {
		ival := ts.GetInt(field_name)
		return NewConstantWithInt(&ival)
	} else {
		sval := ts.GetString(field_name)
		return NewConstantWithString(&sval)
	}
}

func (ts *TableScan) HasField(field_name string) bool {
	return ts.layout.Schema().HasFields(field_name)
}

func (ts *TableScan) SetInt(field_name string, val int) {
	ts.rm.SetInt(ts.current_slot, field_name, val)
}

func (ts *TableScan) SetString(field_name string, val string) {
	ts.rm.SetString(ts.current_slot, field_name, val)
}

func (ts *TableScan) SetVal(field_name string, val *Constant) {
	if ts.layout.Schema().Type(field_name) == record_manager.INTEGER {
		ts.SetInt(field_name, val.AsInt())
	} else {
		ts.SetString(field_name, val.AsString())
	}
}

func (ts *TableScan) Insert() {
	ts.current_slot = ts.rm.InsertAfter(ts.current_slot)
	for ts.current_slot < 0 {
		if ts.AtLastBlock() {
			ts.MoveToNewBlock()
		} else {
			ts.MoveToBlock(int(ts.rm.Block().BlockNum() + 1))
		}
		ts.current_slot = ts.rm.InsertAfter(ts.current_slot)
	}
}

func (ts *TableScan) Delete() {
	ts.rm.Delete(ts.current_slot)
}

func (ts *TableScan) GetRid() record_manager.RecordIdentifierInterface {
	return record_manager.NewRID(int(ts.rm.Block().BlockNum()), ts.current_slot)
}
func (ts *TableScan) MoveToRID(rid record_manager.RecordIdentifierInterface) {
	ts.Close()
	blk := fm.NewBlockID(ts.file_name, uint64(rid.BlockID()))
	ts.rm = record_manager.NewRecordPage(ts.tx, blk, ts.layout)
	ts.current_slot = rid.Slot()
}

func (ts *TableScan) MoveToBlock(bid int) {
	ts.Close()
	blk := fm.NewBlockID(ts.file_name, uint64(bid))
	ts.rm = record_manager.NewRecordPage(ts.tx, blk, ts.layout)
	ts.current_slot = -1
}

func (ts *TableScan) MoveToNewBlock() {
	ts.Close()
	blk, err := ts.tx.Append(ts.file_name)
	if err != nil {
		panic(err)
	}
	ts.rm = record_manager.NewRecordPage(ts.tx, blk, ts.layout)
	ts.rm.Format()
	ts.current_slot = -1
}

func (ts *TableScan) AtLastBlock() bool {
	size, err := ts.tx.Size(ts.file_name)
	if err != nil {
		panic(err)
	}

	return ts.rm.Block().BlockNum() == size-1
}
