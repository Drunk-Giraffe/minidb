package log_manager

import (
	fm "file_manager"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func makeRecord(s string, n uint64) []byte {
	//生成日志内容
	p := fm.NewPageBySize(1)
	npos := p.MaxLengthForString(s)
	b := make([]byte, uint64(npos+8))
	p = fm.NewPageByBytes(b)
	p.SetString(0, s)
	p.SetInt(npos, n)
	return b
}

func createRecords(lm *LogManager, start uint64, end uint64) {
	//生成日志
	for i := start; i <= end; i++ {
		rec := makeRecord(fmt.Sprintf("record %d", i), i)
		lm.AppendLogRecord(rec)
	}

}

func TestLogManager(t *testing.T) {
	file_manager, _ := fm.NewFileManager("log_test", 400)
	log_manager, err := NewLogManager(file_manager, "log_file")
	require.Nil(t, err)

	createRecords(log_manager, 1, 35)

	iter := log_manager.Iterator()
	rec_num := 35
	for iter.HasNext() {
		rec := iter.Next()
		p := fm.NewPageByBytes(rec)
		s := p.GetString(0)

		require.Equal(t, fmt.Sprintf("record %d", rec_num), s)
		npos := p.MaxLengthForString(s)
		val := p.GetInt(npos)
		require.Equal(t, val, uint64(rec_num))
		rec_num--
	}

	createRecords(log_manager, 36, 70)
	log_manager.FlushByLSN(65)

	iter = log_manager.Iterator()
	rec_num = 70
	for iter.HasNext() {
		rec := iter.Next()
		p := fm.NewPageByBytes(rec)
		s := p.GetString(0)
		require.Equal(t, fmt.Sprintf("record %d", rec_num), s)
		npos := p.MaxLengthForString(s)
		val := p.GetInt(npos)
		require.Equal(t, val, uint64(rec_num))
		rec_num--
	}
}
