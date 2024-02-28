package file_manager

import (
	"encoding/binary"
)

type Page struct {
	buffer []byte
}

func NewPageBySize(block_Size uint64) *Page {
	return &Page{
		buffer: make([]byte, block_Size),
	}
}

func NewPageByBytes(bytes []byte) *Page {
	return &Page{
		buffer: bytes,
	}
}

func (p *Page) GetInt(offset uint64) uint64 {
	return binary.LittleEndian.Uint64(p.buffer[offset : offset+8])
}

func (p *Page) SetInt(offset uint64, num uint64) {
	copy(p.buffer[offset:offset+8], uint64ToByteArray(num))
}

func uint64ToByteArray(num uint64) []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, num)
	return bytes
}

func (p *Page) GetBytes(offset uint64) []byte {
	len := binary.LittleEndian.Uint64(p.buffer[offset : offset+8])
	new_buf := make([]byte, len)
	copy(new_buf, p.buffer[offset+8:offset+8+len])
	return new_buf
}

func (p *Page) SetBytes(offset uint64, bytes []byte) {
	len := uint64(len(bytes))
	copy(p.buffer[offset:offset+8], uint64ToByteArray(len))
	copy(p.buffer[offset+8:offset+8+len], bytes)
}

func (p *Page) MaxLengthForString(s string) uint64 {
	b := []byte(s)
	uint64_size := 8
	return uint64(uint64_size + len(b))
}

func (p *Page) GetString(offset uint64) string {
	len := binary.LittleEndian.Uint64(p.buffer[offset : offset+8])
	return string(p.buffer[offset+8 : offset+8+len])
}

func (p *Page) SetString(offset uint64, str string) {
	str_bytes := []byte(str)
	p.SetBytes(offset, str_bytes)
}

func (p *Page) contents() []byte {
	return p.buffer
}
