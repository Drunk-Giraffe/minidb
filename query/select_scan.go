package query

import "record_manager"

type SelectScan struct {
	updateScan UpdateScan
	predicate *Predicate
}

func NewSelectScan(scan UpdateScan, predicate *Predicate) *SelectScan {
	return &SelectScan{scan, predicate}
}

func (ss *SelectScan) PointBeforeFirst() {
	ss.updateScan.GetScan().PointBeforeFirst()
}

func (ss *SelectScan) Next() bool{
	for ss.updateScan.GetScan().Next() {
		if ss.predicate.IsSatisfied(ss) {
			return true
		}
	}
	return false
}

func (ss *SelectScan) GetInt(field string) int {
	return ss.updateScan.GetScan().GetInt(field)
}

func (ss *SelectScan) GetString(field string) string {
	return ss.updateScan.GetScan().GetString(field)
}

func (ss *SelectScan) GetVal(field string) *Constant {
	return ss.updateScan.GetScan().GetVal(field)
}

func (ss *SelectScan) HasField(field string) bool {
	return ss.updateScan.GetScan().HasField(field)
}

func (ss *SelectScan) Close() {
	ss.updateScan.GetScan().Close()
}

func (ss *SelectScan) SetInt(field string, val int) {
	ss.updateScan.SetInt(field, val)
}

func (ss *SelectScan) SetString(field string, val string) {
	ss.updateScan.SetString(field, val)
}

func (ss *SelectScan) SetVal(field string, val *Constant) {
	ss.updateScan.SetVal(field, val)
}

func (ss *SelectScan) Insert() {
	ss.updateScan.Insert()
}

func (ss *SelectScan) Delete() {
	ss.updateScan.Delete()
}

func (ss *SelectScan) GetRid() *record_manager.RID {
	return ss.updateScan.GetRid()
}

func (ss *SelectScan) MoveToRid(rid *record_manager.RID) {
	ss.updateScan.MoveToRid(rid)
}