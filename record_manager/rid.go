package record_manager

import (
	"fmt"
)

type RID struct {
	blk_num int
	slot    int
}

func NewRID(blk_num int, slot int) *RID {
	return &RID{
		blk_num: blk_num,
		slot:    slot,
	}
}

func (r *RID) BlockID() int {
	return r.blk_num
}

func (r *RID) Slot() int {
	return r.slot
}

func (r *RID) Equals(other RecordIdentifierInterface) bool {
	return r.blk_num == other.BlockID() && r.slot == other.Slot()
}

func (r *RID) ToString() string {
	return fmt.Sprintf("[ %d , %d ]", r.blk_num, r.slot)
}
