package tx

import (
	fm "file_manager"
	"fmt"
	lm "log_manager"
)

type SetStringRecord struct {
	txID   uint64
	offset uint64
	val    string
	blk    *fm.BlockID
}

func NewSetStringRecord(p *fm.Page) *SetStringRecord {
	txID := p.GetInt(uint64(8))
	filename := p.GetString(uint64(16))
	blk_id := p.GetInt(uint64(16 + p.MaxLengthForString(filename)))
	blk := fm.NewBlockID(filename, uint64(blk_id))
	offset := p.GetInt(uint64(24 + p.MaxLengthForString(filename)))
	val := p.GetString(uint64(32 + p.MaxLengthForString(filename)))

	return &SetStringRecord{
		txID:   uint64(txID),
		offset: uint64(offset),
		val:    val,
		blk:    blk,
	}
}

func (s *SetStringRecord) Op() RECORD_TYPE {
	return SET_STRING
}

func (s *SetStringRecord) TxID() uint64 {
	return s.txID
}

func (s *SetStringRecord) ToString() string {
	return fmt.Sprintf("<SETSTRING %d, %s, %d, %s>", s.txID, s.blk.FileName(), s.offset, s.val)
}

func (s *SetStringRecord) Undo(tx TransactionInterface) {
	tx.Pin(s.blk)
	tx.SetString(s.blk, s.offset, s.val, false)
	tx.Unpin(s.blk)
}

func WriteSetStringLog(lgmr *lm.LogManager, txID uint64, blk *fm.BlockID, offset uint64, val string) (uint64, error) {
	p := fm.NewPageBySize(1)
	t_pos := uint64(8)
	f_pos := uint64(t_pos + 8)
	b_pos := uint64(f_pos + p.MaxLengthForString(blk.FileName()))
	o_pos := uint64(b_pos + 8)
	v_pos := uint64(o_pos + 8)
	rec_len := uint64(v_pos + p.MaxLengthForString(val))
	rec := make([]byte, rec_len)

	p = fm.NewPageByBytes(rec)
	p.SetInt(0, int64(SET_STRING))
	p.SetInt(t_pos, int64(txID))
	p.SetString(f_pos, blk.FileName())
	p.SetInt(b_pos, int64(blk.BlockNum()))
	p.SetInt(o_pos, int64(offset))
	p.SetString(v_pos, val)

	return lgmr.AppendLogRecord(rec)
}
