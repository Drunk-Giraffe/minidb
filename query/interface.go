package query

import "record_manager"

type Scan interface {
	PointBeforeFirst()
	Next() bool
	GetInt(field string) int
	GetString(field string) string
	GetVal(field string) *Constant
	HasField(field string) bool
	Close()
}

type UpdateScan interface {
	GetScan() Scan
	SetInt(field string, val int)
	SetString(field string, val string)
	SetVal(field string, val *Constant)
	Insert()
	Delete()
	GetRid() *record_manager.RID
	MoveToRid(rid *record_manager.RID)
}

type Plan interface {
	Open() interface{}
	BlocksAccessed() int               //对应 B(s)
	RecordsOutput() int                //对应 R(s)
	DistinctValues(fldName string) int //对应 V(s,F)
	Schema() record_manager.SchemaInterface
}
