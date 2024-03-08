package tx

import (
	"encoding/binary"
	fm "file_manager"
	"fmt"
	lm "log_manager"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStartRecord(t *testing.T) {
	//初始化文件管理器
	file_manager, err := fm.NewFileManager("recordtest", 400)
	log_manager, err := lm.NewLogManager(file_manager, "recordtestlog")

	tx_num := uint64(1)
	p := fm.NewPageBySize(32)
	p.SetInt(uint64(0), int64(START))
	p.SetInt(uint64(8), int64(tx_num))
	start_record := NewStartRecord(p, log_manager)
	require.Equal(t, start_record.ToString(), fmt.Sprintf("<START %d>", tx_num))

	_, err = start_record.WriteStartLog()
	require.Nil(t, err)

	iter := log_manager.Iterator()
	rec := iter.Next()
	rec_op := binary.LittleEndian.Uint64(rec[:8])
	rec_tx_num := binary.LittleEndian.Uint64(rec[8:16])
	require.Equal(t, rec_op, uint64(START))
	require.Equal(t, rec_tx_num, tx_num)
}

func TestSetStringRecord(t *testing.T) {
	file_manager, _ := fm.NewFileManager("recordtest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "setstringlog")
	str := "test"
	blk_id := uint64(1)
	dummy_blk := fm.NewBlockID("dummy_id", blk_id)
	tx_num := uint64(1)
	offset := uint64(16)

	WriteSetStringLog(log_manager, tx_num, dummy_blk, offset, str)
	p := fm.NewPageBySize(400)
	p.SetString(offset, str)

	iter := log_manager.Iterator()
	rec := iter.Next()
	log_page := fm.NewPageByBytes(rec)
	set_string_record := NewSetStringRecord(log_page)
	require.Equal(t, set_string_record.ToString(), fmt.Sprintf("<SETSTRING %d, %s, %d, %s>", tx_num, dummy_blk.FileName(), offset, str))

	p.SetString(offset, "test2")
	p.SetString(offset, "test3")
	txStub := NewTxStub(p)
	set_string_record.Undo(txStub)
	recover_str := p.GetString(offset)
	require.Equal(t, recover_str, str)
}

func TestSetIntRecord(t *testing.T) {
	file_manager, _ := fm.NewFileManager("recordtest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "setintlog")
	val := int64(888)
	blk_id := uint64(1)
	dummy_blk := fm.NewBlockID("dummy_id", blk_id)
	tx_num := uint64(1)
	offset := uint64(8)

	WriteSetIntLog(log_manager, tx_num, dummy_blk, offset, val)
	p := fm.NewPageBySize(400)
	p.SetInt(offset, val)

	iter := log_manager.Iterator()
	rec := iter.Next()
	log_page := fm.NewPageByBytes(rec)
	set_int_record := NewSetIntRecord(log_page)
	require.Equal(t, set_int_record.ToString(), fmt.Sprintf("<SETINT %d, %s, %d, %d>", tx_num, dummy_blk.FileName(), offset, val))

	p.SetInt(offset, int64(999))
	p.SetInt(offset, int64(777))
	txStub := NewTxStub(p)
	set_int_record.Undo(txStub)
	recover_val := p.GetInt(offset)

	require.Equal(t, recover_val, val)
}

func TestRollbackRecord(t *testing.T) {
	file_manager, _ := fm.NewFileManager("recordtest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "rollback")
	tx_num := uint64(13)
	WriteRollbackLog(log_manager, tx_num)
	iter := log_manager.Iterator()
	rec := iter.Next()
	pp := fm.NewPageByBytes(rec)

	roll_back_rec := NewRollbackRecord(pp)
	expected_str := fmt.Sprintf("<ROLLBACK %d>", tx_num)

	require.Equal(t, expected_str, roll_back_rec.ToString())
}

func TestCommitRecord(t *testing.T) {
	file_manager, _ := fm.NewFileManager("recordtest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "commit")
	tx_num := uint64(13)
	WriteCommitLog(log_manager, tx_num)
	iter := log_manager.Iterator()
	rec := iter.Next()
	pp := fm.NewPageByBytes(rec)

	roll_back_rec := NewCommitRecord(pp)
	expected_str := fmt.Sprintf("<COMMIT %d>", tx_num)

	require.Equal(t, expected_str, roll_back_rec.ToString())
}

func TestCheckPointRecord(t *testing.T) {
	file_manager, _ := fm.NewFileManager("recordtest", 400)
	log_manager, _ := lm.NewLogManager(file_manager, "checkpoint")
	WriteCheckPointLog(log_manager)
	iter := log_manager.Iterator()
	rec := iter.Next()
	pp := fm.NewPageByBytes(rec)
	val := pp.GetInt(0)

	require.Equal(t, val, uint64(CHECKPOINT))

	check_point_rec := NewCheckPointRecord()
	expected_str := "<CHECKPOINT>"
	require.Equal(t, expected_str, check_point_rec.ToString())
}
