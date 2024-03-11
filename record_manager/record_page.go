package record_manager

import (
	fm "file_manager"
	// "fmt"
	"tx"
)

type SLOT_FLAG int

const (
	EMPTY SLOT_FLAG = iota
	USED
)

type RecordPage struct {
	tx     *tx.Transaction
	blk    *fm.BlockID
	layout LayoutInterface
}

func NewRecordPage(tx *tx.Transaction, blk *fm.BlockID, layout LayoutInterface) *RecordPage {
	rp := &RecordPage{
		tx:     tx,
		blk:    blk,
		layout: layout,
	}
	tx.Pin(blk)
	return rp

}

func (r *RecordPage) offset(slot int) uint64 {
	return uint64(slot * r.layout.SlotSize())
}

func (r *RecordPage) GetInt(slot int, field_name string) int {
	field_pos := r.offset(slot) + uint64(r.layout.Offset(field_name))
	val, err := r.tx.GetInt(r.blk, field_pos)
	if err == nil {
		return int(val)
	}

	return -1
}

func (r *RecordPage) GetString(slot int, field_name string) string {
	field_pos := r.offset(slot) + uint64(r.layout.Offset(field_name))
	val, _ := r.tx.GetString(r.blk, field_pos)
	return val
}

func (r *RecordPage) SetInt(slot int, field_name string, val int) {
	field_pos := r.offset(slot) + uint64(r.layout.Offset(field_name))
	r.tx.SetInt(r.blk, field_pos, int64(val), true)
}

func (r *RecordPage) SetString(slot int, field_name string, val string) {
	field_pos := r.offset(slot) + uint64(r.layout.Offset(field_name))
	r.tx.SetString(r.blk, field_pos, val, true)
}

func (r *RecordPage) Delete(slot int) {
	r.setFlag(slot, EMPTY)
}

func (r *RecordPage) Format() {
	slot := 0
	for r.isValidSlot(slot) {
		// 将当前槽位标记为 EMPTY
		// fmt.Println("formatting slot ", slot)
		err := r.tx.SetInt(r.blk, r.offset(slot), int64(EMPTY), false)
		if err != nil {
			// fmt.Println("formatting slot ", slot, " error: ", err)
		}
		// fmt.Println("formatting slot ", slot, " done")
		// 获取当前布局的schema
		sch := r.layout.Schema()
		// 遍历schema中的所有字段
		for _, field_name := range sch.Fields() {
			// 计算字段在当前槽位的偏移位置
			field_pos := r.offset(slot) + uint64(r.layout.Offset(field_name))
			// 根据字段类型设置默认值
			if sch.Type(field_name) == INTEGER {
				// fmt.Println("formatting slot ", slot, " field ", field_name)
				r.tx.SetInt(r.blk, field_pos, 0, false)
				// fmt.Println("formatting slot ", slot, " field ", field_name, " done")
			} else {
				// fmt.Println("formatting slot ", slot, " field ", field_name)
				r.tx.SetString(r.blk, field_pos, "", false)
				// fmt.Println("formatting slot ", slot, " field ", field_name, " done")
			}
		}
		// 移至下一个槽位进行初始化
		slot += 1
	}
}

func (r *RecordPage) NextAfter(slot int) int {
	return r.searchAfter(slot, USED)
}

func (r *RecordPage) InsertAfter(slot int) int {
	new_slot := r.searchAfter(slot, EMPTY)
	if new_slot >= 0 {
		r.setFlag(new_slot, USED)
	}

	return new_slot
}

func (r *RecordPage) Block() *fm.BlockID {
	return r.blk
}

func (r *RecordPage) setFlag(slot int, flag SLOT_FLAG) {
	r.tx.SetInt(r.blk, r.offset(slot), int64(flag), true)
}

func (r *RecordPage) searchAfter(slot int, flag SLOT_FLAG) int {
	slot += 1
	for r.isValidSlot(slot) {
		val, err := r.tx.GetInt(r.blk, r.offset(slot))
		if err != nil {
			return -1
		}
		if SLOT_FLAG(val) == flag {
			return slot
		}
		slot += 1
	}

	return -1
}

func (r *RecordPage) isValidSlot(slot int) bool {
	return r.offset(slot+1) <= r.tx.BlockSize()
}
