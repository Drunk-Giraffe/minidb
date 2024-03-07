package tx

import (
	fm "file_manager"
)

type TxStub struct {
	p *fm.Page
}

func NewTxStub(p *fm.Page) *TxStub {
	return &TxStub{
		p: p,
	}
}

func (t *TxStub) Commit() {

}

func (t *TxStub) Rollback() {

}

func (t *TxStub) Recover() {

}

func (t *TxStub) Pin(_ *fm.BlockID) {

}

func (t *TxStub) Unpin(_ *fm.BlockID) {

}
func (t *TxStub) GetInt(_ *fm.BlockID, offset uint64) uint64 {

	return t.p.GetInt(offset)
}

func (t *TxStub) GetString(_ *fm.BlockID, offset uint64) string {
	val := t.p.GetString(offset)
	return val
}

func (t *TxStub) SetInt(_ *fm.BlockID, offset uint64, val uint64, _ bool) {
	t.p.SetInt(offset, val)
}

func (t *TxStub) SetString(_ *fm.BlockID, offset uint64, val string, _ bool) {
	t.p.SetString(offset, val)
}

func (t *TxStub) AvailableBuffers() uint64 {
	return 0
}

func (t *TxStub) Size(_ string) uint64 {
	return 0
}

func (t *TxStub) Append(_ string) *fm.BlockID {
	return nil
}

func (t *TxStub) BlockSize() uint64 {
	return 0
}
