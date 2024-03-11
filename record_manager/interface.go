package record_manager

import (
	fm "file_manager"
)

type SchemaInterface interface {
	AddField(field_name string, field_type FIELD_TYPE, length int)
	AddIntField(field_name string)
	AddStringField(field_name string, length int)
	Add(field_name string, other_sch SchemaInterface)
	AddAll(sch SchemaInterface)
	Fields() []string
	HasFields(field_name string) bool
	Type(field_name string) FIELD_TYPE
	Length(field_name string) int
}

type LayoutInterface interface {
	Schema() SchemaInterface
	Offset(field_name string) int
	SlotSize() int
}

type RecordManagerInterface interface {
	Block() *fm.BlockID
	GetInt(slot int, field_name string) int
	GetString(slot int, field_name string) string
	SetInt(slot int, field_name string, value int)
	SetString(slot int, field_name string, value string)
	Format()                  //将一个块格式化为一个空的记录块
	Delete(slot int)          //删除一个记录
	NextAfter(slot int) int   //返回slot之后的第一个flag为1记录
	InsertAfter(slot int) int //返回slot之后的第一个flag为0记录
}

type RecordIdentifierInterface interface {
	BlockID() int
	Slot() int
	ToString() string
}

type TableScanInterface interface {
	Close()
	HasField(field_name string) bool
	PutBeforeFirst() //将指针放在第一条记录前
	Next() bool
	MoveToRid(rid RecordIdentifierInterface) //跳转到指定目录
	Insert()

	GetInt(field_name string) int
	GetString(field_name string) string
	SetInt(field_name string, value int)
	SetString(field_name string, value string)
	CurrentRID() RecordIdentifierInterface
	Delete()
}
