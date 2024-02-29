package log_manager

import (
	fm "file_manager"
	"sync"
)

type LogManager struct {
	file_manager   *fm.FileManager
	log_file       string
	log_page       *fm.Page //存储日志的页
	current_blk    *fm.BlockID
	latest_lsn     uint64 //最新的日志序列号
	last_saved_log uint64 //上一次保存的日志序列号
	mu             *sync.Mutex
}

// 缓冲区用完后为缓冲区分配新的块
func (l *LogManager) appendNewBlock(*fm.BlockID, error) {
	blk, err := l.file_manager.AppendBlock(l.log_file)
	if err != nil {
		return nil, err
	}

	// 日志自底向上写入，所以要把写入内容的偏移量写在缓冲区的前8个字节

	l.log_page.SetInt(0, uint64(l.file_manager.BlockSize()))
	l.file_manager.WriteBlock(&blk, l.log_page)
	return &blk, nil
}

// 初始化日志管理器
func NewLogManager(file_manager *fm.FileManager, log_file string) (*LogManager, error) {
	log_mgr := LogManager{
		file_manager:   file_manager,
		log_file:       log_file,
		log_page:       fm.NewPageBySize(file_manager.BlockSize()),
		last_saved_log: 0,
		latest_lsn:     0,
	}

	log_size, err := file_manager.Size(log_file)
	if err != nil {
		return nil, err
	}

	if log_size == 0 {
		blk, err := log_mgr.appendNewBlock()
		if err != nil {
			return nil, err
		}
		log_mgr.current_blk = blk
	} else {
		//若文件存在，先把末尾的日志块读入缓冲区，若缓冲区未满，直接写入，否则为缓冲区分配新的块
		log_mgr.current_blk = fm.NewBlockID(log_mgr.log_file, log_size-1)
		log_mgr.file_manager.Read(log_mgr.current_blk, log_mgr.log_page)
	}
	return &log_mgr, nil
}

// LSN是日志序列号，用于标识日志的顺序
func (l *LogManager) FlushByLSN(ls uint64) error {
	//把给定的LSN及之前的日志写入磁盘
	//写入给定编号的日志块时，与当前日志处于同一块的日志也会被写入
	if l.last_saved_log >= ls {
		err := l.Flush()
		if err != nil {
			return err
		}
		l.last_saved_log = l.latest_lsn
	}
	return nil
}

// 把缓冲区的日志写入磁盘
func (l *LogManager) Flush() error {
	//把缓冲区的日志写入磁盘
	//写入给定编号的日志块时，与当前日志处于同一块的日志也会被写入
	_, err := l.file_manager.Write(l.current_blk, l.log_page)
	if err != nil {
		return err
	}
	return nil
}

func (l *LogManager) AppendLogRecord(log_record []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	boundary := l.log_page.GetInt(0)
	record_size := len(log_record)
	bytes_needed := record_size + 8
	var err error
	if int(boundary-bytes_needed) < 8 {
		//缓冲区剩余空间不足,先把缓冲区的日志写入磁盘
		err = l.Flush()
		if err != nil {
			return l.latest_lsn, err
		}

		//为缓冲区分配新的块
		l.current_blk, err = l.appendNewBlock()
		if err != nil {
			return l.latest_lsn, err
		}
	}
	boundary = l.log_page.GetInt(0)
	record_pos := boundary - bytes_needed
	l.log_page.SetInt(0, record_pos)
	l.log_page.SetBytes(record_pos, log_record)
	l.latest_lsn++
	return l.latest_lsn, nil
}

func (l *LogManager) Iterator() *LogIterator {
	l.Flush()
	return NewLogIterator(l.file_manager, l.current_blk)
}
