package file_manager

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// FileManager is a struct that contains the file and the mutex for the file.
type FileManager struct {
	db_directory string
	block_Size   uint64
	is_new       bool
	open_files   map[string]*os.File
	mu           sync.Mutex
}

func NewFileManager(db_directory string, block_Size uint64) (*FileManager, error) {
	file_manager := FileManager{
		db_directory: db_directory,
		block_Size:   block_Size,
		is_new:       false,
		open_files:   make(map[string]*os.File),
	}
	if _, err := os.Stat(db_directory); os.IsNotExist(err) {
		file_manager.is_new = true
		err := os.MkdirAll(db_directory, os.ModeDir)
		if err != nil {
			return nil, err
		}
	} else {
		err := filepath.Walk(db_directory, func(path string, info os.FileInfo, err error) error {
			mode := info.Mode()
			if mode.IsRegular() {
				name := info.Name()
				if strings.HasPrefix(name, "temp") {
					os.Remove(filepath.Join(path, name))
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}

	}
	return &file_manager, nil
}

func (fm *FileManager) getFile(file_name string) (*os.File, error) {
	path := filepath.Join(fm.db_directory, file_name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	fm.open_files[file_name] = file
	return file, nil
}

func (fm *FileManager) Read(blk *BlockID, page *Page) (int, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	file, err := fm.getFile(blk.FileName())
	if err != nil {
		return 0, err
	}
	defer file.Close()
	count, err := file.ReadAt(page.contents(), int64(blk.BlockNum()*fm.block_Size))
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (fm *FileManager) Write(blk *BlockID, page *Page) (int, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	file, err := fm.getFile(blk.FileName())
	if err != nil {
		return 0, err
	}
	defer file.Close()
	count, err := file.WriteAt(page.contents(), int64(blk.BlockNum()*fm.block_Size))
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (fm *FileManager) Size(file_name string) (uint64, error) {
	file, err := fm.getFile(file_name)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	fi, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return uint64(fi.Size() / int64(fm.block_Size)), nil
}

func (fm *FileManager) Append(file_name string) (BlockID, error) {
	new_block_num, err := fm.Size(file_name)
	if err != nil {
		return BlockID{}, err
	}
	blk := NewBlockID(file_name, new_block_num)
	file, err := fm.getFile(blk.FileName())
	if err != nil {
		return BlockID{}, err
	}
	defer file.Close()

	b := make([]byte, fm.block_Size)
	_, err = file.WriteAt(b, int64(blk.BlockNum()*fm.block_Size))
	if err != nil {
		return BlockID{}, err
	}
	return *blk, nil

}

func (fm *FileManager) IsNew() bool {
	return fm.is_new
}

func (fm *FileManager) BlockSize() uint64 {
	return fm.block_Size
}
