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
func (t *TxStub) GetInt(_ *fm.BlockID, offset uint64) (int64, error) {

	return t.p.GetInt(offset), nil
}

func (t *TxStub) GetString(_ *fm.BlockID, offset uint64) (string, error) {
	val := t.p.GetString(offset)
	return val, nil
}

func (t *TxStub) SetInt(_ *fm.BlockID, offset uint64, val int64, _ bool) error {
	t.p.SetInt(offset, val)
	return nil
}

func (t *TxStub) SetString(_ *fm.BlockID, offset uint64, val string, _ bool) error {
	t.p.SetString(offset, val)
	return nil
}

func (t *TxStub) AvailableBuffers() uint64 {
	return 0
}

func (t *TxStub) Size(_ string) (int64, error) {
	return 0, nil
}

func (t *TxStub) Append(_ string) (*fm.BlockID, error) {
	return nil, nil
}

func (t *TxStub) BlockSize() uint64 {
	return 0
}
