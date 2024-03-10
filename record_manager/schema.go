package record_manager

type FIELD_TYPE int

const (
	INTEGER FIELD_TYPE = iota
	VARCHAR
)

type FieldInfo struct {
	field_type FIELD_TYPE
	length     int
}

func newFieldInfo(field_type FIELD_TYPE, length int) *FieldInfo {
	return &FieldInfo{
		field_type: field_type,
		length:     length,
	}
}

type Schema struct {
	fields []string
	infos  map[string]*FieldInfo
}

func NewSchema() *Schema {
	return &Schema{
		fields: make([]string, 0),
		infos:  make(map[string]*FieldInfo),
	}
}

func (sch *Schema) AddField(field_name string, field_type FIELD_TYPE, length int) {
	sch.fields = append(sch.fields, field_name)
	sch.infos[field_name] = newFieldInfo(field_type, length)
}

func (sch *Schema) AddIntField(field_name string) {
	sch.AddField(field_name, INTEGER, 0)
}

func (sch *Schema) AddStringField(field_name string, length int) {
	sch.AddField(field_name, VARCHAR, length)
}

func (sch *Schema) HasFields(field_name string) bool {
	for _, field := range sch.fields {
		if field == field_name {
			return true
		}
	}
	return false
}

func (sch *Schema) Add(field_name string, other_sch SchemaInterface) {
	field_type := other_sch.Type(field_name)
	length := other_sch.Length(field_name)
	sch.AddField(field_name, field_type, length)
}

func (sch *Schema) AddAll(other_sch SchemaInterface) {
	for _, field := range other_sch.Fields() {
		sch.Add(field, other_sch)
	}
}

func (sch *Schema) Fields() []string {
	return sch.fields
}

func (sch *Schema) Type(field_name string) FIELD_TYPE {
	return sch.infos[field_name].field_type
}

func (sch *Schema) Length(field_name string) int {
	return sch.infos[field_name].length
}
