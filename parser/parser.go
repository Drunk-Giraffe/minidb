package parser

import (
	"fmt"
	"lexer"
	"query"
	"record_manager"
	"strconv"
	"strings"

	"golang.org/x/tools/go/analysis/passes/nilfunc"
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
		return query.ConstantWithInt(i)
	case lexer.STRING:
		s := strings.Clone(sp.sqlLexer.Lexeme)
		return query.NewConstantWithString(s)
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
	tok, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if tok.Tag != lexer.ASSIGN_OPERATOR {
		panic("Expecting =")
	}

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
	token, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	if token.Tag != lexer.SELECT {
		panic("Expecting SELECT")
	}

	fields := sp.SelectList()
	token, err = sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if token.Tag != lexer.FROM {
		panic("Expecting FROM")
	}

	tables := sp.TableList()
	token, err = sp.sqlLexer.Scan()
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
	}else if token.Tag == lexer.UPDATE {
		return nil
	}else if token.Tag == lexer.DELETE {
		return nil
	}else {
		sp.sqlLexer.ReverseScan()
		return sp.Create()
	}
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
	token, err := sp.sqlLexer.Scan()
	
	if err != nil {
		panic(err)
	}
	if token.Tag != lexer.CREATE {
		panic("Expecting CREATE")
	}
	token, err = sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if token.Tag == lexer.TABLE {
		return sp.CreateTable()
	}else if token.Tag == lexer.View() {
		return nil
	}else {
		return nil
	}
}

func (sp *SQLParser) CreateTable() *CreateTableData {
	token, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if token.Tag != lexer.ID {
		panic("Expecting table name")
	}
	table_name := sp.sqlLexer.Lexeme
	token, err = sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if token.Tag != lexer.LEFT_BRACKET {
		panic("Expecting (")
	}

	sch := sp.FieldDefs()
	token, err = sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	if token.Tag != lexer.RIGHT_BRACKET {
		panic("Expecting )")
	}

	return NewCreateTableData(table_name, sch)
}

func (sp *SQLParser) FieldDefs() *record_manager.Schema{
	schema := sp.FieldDefs()
	token, err := sp.sqlLexer.Scan()

	if err != nil{
		panic(err)
	}
	if token.Tag == lexer.COMMA {
		schema2 := sp.FieldDefs()
		schema.AddAll(schema2)
	}else {
		sp.sqlLexer.ReverseScan()
	}
	return schema
}


func (sp *SQLParser) FieldDef() *record_manager.Schema {
	_, field_name := sp.Field()
	return sp.FieldType(field_name)
}

func (sp *SQLParser) FieldType(field_name string) *record_manager.Schema  {
	schema := record_manager.NewSchema()
	token, err := sp.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if token.Tag == lexer.INT {
		schema.AddIntField(field_name)
	}else if token.Tag == lexer.VARCHAR {
		token, err = sp.sqlLexer.Scan()
		if err != nil {
			panic(err)
		}
		if token.Tag != lexer.LEFT_BRACKET {
			panic("Expecting (")
		}
		token, err = sp.sqlLexer.Scan()
		if err != nil {
			panic(err)
		}
		if token.Tag != lexer.NUM {
			panic("Expecting number")
		}
		num := sp.sqlLexer.Lexeme
		field_len, err := strconv.Atoi(num)
		schema.AddStringField(field_name, field_len)
		token, err = sp.sqlLexer.Scan()
		if err != nil {
			panic(err)
		}
		if token.Tag != lexer.RIGHT_BRACKET {
			panic("Expecting )")
		}
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