package parser

import (
	"fmt"
	"lexer"
	"query"
	"record_manager"
	"strconv"
	"strings"
)

type SQLParser struct {
	sqlLexer lexer.Lexer
}

func NewSQLParser(s string) *SQLParser {
	return &SQLParser{lexer.NewLexer(s)}
}

func (sp *SQLParser) Field() (lexer.Token, string) {
	tok, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	if tok.Tag != lexer.ID {
		panic("Expecting field name")
	}
	return tok, sp.sqlLexer.Lexeme
}

func (sp *SQLParser) Constant() *query.Constant {
	tok, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	switch tok.Tag {
	case lexer.NUM:
		i, err := strconv.Atoi(sp.sqlLexer.Lexeme)
		if err != nil {
			panic(err)
		}
		return query.NewConstantWithInt(&i)
	case lexer.STRING:
		s := strings.Clone(sp.sqlLexer.Lexeme)
		return query.NewConstantWithString(&s)
	default:
		panic("Expecting constant")
	}
}

func (sp *SQLParser) Expression() *query.Expression {
	tok, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	if tok.Tag == lexer.ID {
		sp.sqlLexer.ReverseScan()
		_, str := sp.Field()
		return query.NewExpressionWithString(str)
	} else {
		sp.sqlLexer.ReverseScan()
		return query.NewExpressionWithConstant(sp.Constant())
	}
}

func (sp *SQLParser) Term() *query.Term {
	lhs := sp.Expression()
	sp.checkToken(lexer.ASSIGN_OPERATOR)

	rhs := sp.Expression()
	return query.NewTerm(lhs, rhs)
}

func (sp *SQLParser) Predicate() *query.Predicate {
	pred := query.NewPredicateWithTerms(sp.Term())
	tok, err := sp.sqlLexer.Scan()

	if err != nil && fmt.Sprint(err) != "EOF" {
		panic(err)
	}

	if tok.Tag == lexer.AND {
		pred.ConjoinWith(sp.Predicate())
	}

	return pred
}

func (sp *SQLParser) Query() *Querydata {
	sp.checkToken(lexer.SELECT)

	fields := sp.SelectList()
	sp.checkToken(lexer.FROM)

	tables := sp.TableList()
	token, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	pred := query.NewPredicate()
	if token.Tag == lexer.WHERE {
		pred = sp.Predicate()
	} else {
		sp.sqlLexer.ReverseScan()
	}
	return NewQuerydata(fields, tables, pred)
}

func (sp *SQLParser) Updatecmd() interface{} {
	token, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if token.Tag == lexer.INSERT {
		return sp.Insert()
	} else if token.Tag == lexer.UPDATE {
		sp.sqlLexer.ReverseScan()
		return sp.Modify()
	} else if token.Tag == lexer.DELETE {
		sp.sqlLexer.ReverseScan()
		return sp.Delete()
	} else {
		sp.sqlLexer.ReverseScan()
		return sp.Create()
	}
}

func (sp *SQLParser) Modify() interface{} {
	sp.checkToken(lexer.UPDATE)
	sp.checkToken(lexer.ID)
	tableName := sp.sqlLexer.Lexeme
	sp.checkToken(lexer.SET)
	_, field := sp.Field()
	sp.checkToken(lexer.ASSIGN_OPERATOR)
	value := sp.Expression()
	pred := query.NewPredicate()
	if sp.isMatch(lexer.WHERE) {
		pred = sp.Predicate()
	}
	return NewModifyData(tableName, field, value, pred)
}

func (sp *SQLParser) Delete() interface{} {
	sp.checkToken(lexer.DELETE)
	sp.checkToken(lexer.FROM)
	sp.checkToken(lexer.ID)
	tableName := sp.sqlLexer.Lexeme
	pred := query.NewPredicate()
	if sp.isMatch(lexer.WHERE) {
		pred = sp.Predicate()
	}
	return NewDeleteData(tableName, pred)
}

func (sp *SQLParser) Insert() interface{} {
	sp.checkToken(lexer.INSERT)
	sp.checkToken(lexer.INTO)
	sp.checkToken(lexer.ID)
	tableName := sp.sqlLexer.Lexeme
	sp.checkToken(lexer.LEFT_BRACKET)
	fields := sp.fieldList()
	sp.checkToken(lexer.RIGHT_BRACKET)
	sp.checkToken(lexer.LEFT_BRACKET)
	sp.checkToken(lexer.VALUES)
	vals := sp.constList()
	sp.checkToken(lexer.RIGHT_BRACKET)
	return NewInsertData(tableName, fields, vals)
}

func (sp *SQLParser) Create() interface{} {
	sp.checkToken(lexer.CREATE)
	token, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if token.Tag == lexer.TABLE {
		return sp.CreateTable()
	} else if token.Tag == lexer.VIEW {
		return sp.CreateView()
	} else if token.Tag == lexer.INDEX {
		return sp.CreateIndex()
	}
	return nil
}

func (sp *SQLParser) CreateIndex() interface{} {
	sp.checkToken(lexer.ID)
	indexName := sp.sqlLexer.Lexeme
	sp.checkToken(lexer.ON)
	sp.checkToken(lexer.ID)
	tableName := sp.sqlLexer.Lexeme
	sp.checkToken(lexer.LEFT_BRACKET)
	_, fieldName := sp.Field()
	sp.checkToken(lexer.RIGHT_BRACKET)
	return NewIndexData(indexName, tableName, fieldName)
}

func (sp *SQLParser) CreateView() interface{} {
	sp.checkToken(lexer.ID)
	view_name := sp.sqlLexer.Lexeme
	sp.checkToken(lexer.AS)
	query := sp.Query()
	vd := NewViewData(view_name, query)
	return vd

}

func (sp *SQLParser) CreateTable() interface{} {
	sp.checkToken(lexer.ID)
	table_name := sp.sqlLexer.Lexeme
	sp.checkToken(lexer.LEFT_BRACKET)

	sch := sp.FieldDefs()
	sp.checkToken(lexer.RIGHT_BRACKET)

	return NewCreateTableData(table_name, sch)
}

func (sp *SQLParser) FieldDefs() *record_manager.Schema {
	schema := sp.FieldDefs()
	token, err := sp.sqlLexer.Scan()

	if err != nil {
		panic(err)
	}
	if token.Tag == lexer.COMMA {
		schema2 := sp.FieldDefs()
		schema.AddAll(schema2)
	} else {
		sp.sqlLexer.ReverseScan()
	}
	return schema
}

func (sp *SQLParser) FieldDef() *record_manager.Schema {
	_, field_name := sp.Field()
	return sp.FieldType(field_name)
}

func (sp *SQLParser) FieldType(field_name string) *record_manager.Schema {
	schema := record_manager.NewSchema()
	token, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if token.Tag == lexer.INT {
		schema.AddIntField(field_name)
	} else if token.Tag == lexer.VARCHAR {
		sp.checkToken(lexer.LEFT_BRACKET)
		sp.checkToken(lexer.NUM)
		num := sp.sqlLexer.Lexeme
		field_len, _ := strconv.Atoi(num)
		schema.AddStringField(field_name, field_len)
		sp.checkToken(lexer.RIGHT_BRACKET)
	}
	return schema
}

func (sp *SQLParser) fieldList() []string {
	L := make([]string, 0)
	_, field := sp.Field()
	L = append(L, field)
	if sp.isMatch(lexer.COMMA) {
		fields := sp.fieldList()
		L = append(L, fields...)
	}

	return L
}

func (sp *SQLParser) constList() []*query.Constant {
	L := make([]*query.Constant, 0)
	L = append(L, sp.Constant())
	if sp.isMatch(lexer.COMMA) {
		consts := sp.constList()
		L = append(L, consts...)
	}

	return L
}

func (sp *SQLParser) checkToken(t lexer.Tag) {
	token, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if token.Tag != t {
		panic("token not match")
	}
}

func (sp *SQLParser) isMatch(t lexer.Tag) bool {
	token, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if token.Tag != t {
		return false
	}
	return true
}

func (sp *SQLParser) SelectList() []string {
	l := make([]string, 0)
	_, field := sp.Field()
	l = append(l, field)
	if sp.isMatch(lexer.COMMA) {
		fields := sp.SelectList()
		l = append(l, fields...)
	} else {
		sp.sqlLexer.ReverseScan()
	}

	return l
}

func (sp *SQLParser) TableList() []string {
	l := make([]string, 0)
	sp.checkToken(lexer.ID)
	l = append(l, sp.sqlLexer.Lexeme)
	if sp.isMatch(lexer.COMMA) {
		tables := sp.TableList()
		l = append(l, tables...)
	} else {
		sp.sqlLexer.ReverseScan()
	}
	return l
}
