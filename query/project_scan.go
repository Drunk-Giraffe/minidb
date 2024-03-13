package query

import (
	"errors"
)

type ProjectScan struct {
	scan      Scan
	fieldList []string
}

func NewProductionScan(s Scan, fieldList []string) *ProjectScan {
	return &ProjectScan{
		scan:      s,
		fieldList: fieldList,
	}
}

func (p *ProjectScan) PointBeforeFirst() {
	p.scan.PointBeforeFirst()
}

func (p *ProjectScan) Next() bool {
	return p.scan.Next()
}

func (p *ProjectScan) GetInt(fldName string) (int, error) {
	if p.scan.HasField(fldName) {
		return p.scan.GetInt(fldName), nil
	}

	return 0, errors.New("Field Not Found")
}

func (p *ProjectScan) GetString(fldName string) (string, error) {
	if p.scan.HasField(fldName) {
		return p.scan.GetString(fldName), nil
	}

	return "", errors.New("Field Not Found")
}

func (p *ProjectScan) GetVal(fldName string) (*Constant, error) {
	if p.scan.HasField(fldName) {
		return p.scan.GetVal(fldName), nil
	}

	return nil, errors.New("Field Not Found")
}

func (p *ProjectScan) HasField(fieldName string) bool {
	for _, s := range p.fieldList {
		if s == fieldName {
			return true
		}
	}

	return false
}

func (p *ProjectScan) Close() {
	p.scan.Close()
}

