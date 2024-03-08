package file_manager

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetGetInt(t *testing.T) {
	page := NewPageBySize(256)
	val := int64(1234)
	offset := uint64(23)
	page.SetInt(offset, val)
	val_got := page.GetInt(offset)
	require.Equal(t, val, val_got)
}

func TestSetGetBytes(t *testing.T) {
	page := NewPageBySize(256)
	bs := []byte{1, 2, 3, 4, 5, 6}
	offset := uint64(111)
	page.SetBytes(offset, bs)
	bs_got := page.GetBytes(offset)
	require.Equal(t, bs, bs_got)
}

func TestSetGetString(t *testing.T) {
	page := NewPageBySize(256)
	str := "hello world"
	offset := uint64(111)
	page.SetString(offset, str)
	str_got := page.GetString(offset)
	require.Equal(t, str, str_got)
}

func TestMaxLengthForString(t *testing.T) {
	str := "hello world"
	s_len := uint64(len([]byte(str)))
	page := NewPageBySize(256)
	s_len_got := page.MaxLengthForString(str)
	require.Equal(t, s_len+8, s_len_got)
}

func TestGetContent(t *testing.T) {
	bs := []byte{1, 2, 3, 4, 5, 6}
	page := NewPageByBytes(bs)
	bs_got := page.contents()
	require.Equal(t, bs, bs_got)
}
