package record_manager

import (
	fm "file_manager"
)

const (
	BYTES_OF_INT = 8
)

type Layout struct {
	schema    SchemaInterface
	offsets   map[string]int
	slot_size int //记录的长度
}

func NewLayoutWithSchema(schema SchemaInterface) *Layout {
	layout := &Layout{
		schema:    schema,
		offsets:   make(map[string]int),
		slot_size: 0,
	}
	fields := schema.Fields()
	pos := uint64(8) //作为slot的标识位
	for i := 0; i < len(fields); i++ {
		layout.offsets[fields[i]] = int(pos)
		pos += uint64(layout.lengthInBytes(fields[i]))
	}
	layout.slot_size = int(pos)
	return layout
}

func NewLayout(schema SchemaInterface, offsets map[string]int, slot_size int) *Layout {
	return &Layout{
		schema:    schema,
		offsets:   offsets,
		slot_size: slot_size,
	}
}

func (layout *Layout) Schema() SchemaInterface {
	return layout.schema
}

func (layout *Layout) LengthInBytes() int {
	return layout.slot_size
}

func (layout *Layout) Offset(field_name string) int {
	offset, ok := layout.offsets[field_name]
	if !ok {
		return -1
	}
	return offset
}

func (layout *Layout) lengthInBytes(field_name string) int {
	field_type := layout.schema.Type(field_name)
	p := fm.NewPageBySize(1)
	if field_type == INTEGER {
		return BYTES_OF_INT
	} else {
		field_length := layout.schema.Length(field_name)
		dummy_str := string(make([]byte, field_length))
		return int(p.MaxLengthForString(dummy_str))
	}
}
