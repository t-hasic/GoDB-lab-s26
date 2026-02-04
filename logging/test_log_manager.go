package logging

import (
	"sync"
	"sync/atomic"
	"time"

	"mit.edu/dsg/godb/common"
)

// NoopLogManager is a no-op implementation of LogManager for testing before recovery is relevant.
type NoopLogManager struct{}

func (n NoopLogManager) Append(record LogRecord) (common.LSN, error) {
	return 0, nil
}

func (n NoopLogManager) WaitUntilFlushed(lsn common.LSN) error {
	return nil
}

func (n NoopLogManager) Iterator(startLSN common.LSN) (LogIterator, error) {
	return nil, nil
}

func (n NoopLogManager) FlushedUntil() common.LSN {
	return 0
}

func (n NoopLogManager) Close() error {
	return nil
}

// MemoryLogManager is an optimized in-memory implementation of LogManager for testing.
// It uses a single flat byte slice to store records to minimize allocation overhead.
type MemoryLogManager struct {
	buffer       []byte
	flushedUntil atomic.Int64
	appendError  atomic.Value
	sync.Mutex
}

func NewMemoryLogManager() *MemoryLogManager {
	return &MemoryLogManager{
		buffer: make([]byte, 0, 4096),
	}
}

func (m *MemoryLogManager) Append(record LogRecord) (common.LSN, error) {
	err := m.appendError.Load().(error)
	if err != nil {
		return 0, err
	}

	m.Lock()
	defer m.Unlock()
	lsn := len(m.buffer)
	m.buffer = append(m.buffer, record.data...)
	return common.LSN(lsn), nil
}

func (m *MemoryLogManager) WaitUntilFlushed(lsn common.LSN) error {
	for m.flushedUntil.Load() < int64(lsn) {
		time.Sleep(time.Millisecond)
	}
	return nil
}

// Iterator returns a scanner to walk the log from a specific starting point.
func (m *MemoryLogManager) Iterator(startLSN common.LSN) (LogIterator, error) {
	return &MemoryLogIterator{
		mgr:        m,
		currOffset: int(startLSN),
	}, nil
}

func (m *MemoryLogManager) FlushedUntil() common.LSN {
	return common.LSN(m.flushedUntil.Load())
}

func (m *MemoryLogManager) Close() error {
	return nil
}

func (m *MemoryLogManager) SetFlushedLSN(lsn common.LSN) {
	m.flushedUntil.Store(int64(lsn))
}

func (m *MemoryLogManager) SetAppendError(err error) {
	m.appendError.Store(err)
}

type MemoryLogIterator struct {
	mgr        *MemoryLogManager
	currOffset int
	current    LogRecord
	err        error
}

func (i *MemoryLogIterator) Next() bool {
	if !i.current.IsNil() {
		i.currOffset += i.current.Size()
	}
	i.current, i.err = AsVerifiedLogRecord(i.mgr.buffer[i.currOffset:])
	if i.err != nil {
		return false
	}
	return true
}

func (i *MemoryLogIterator) CurrentRecord() LogRecord {
	return i.current
}

func (i *MemoryLogIterator) CurrentLSN() common.LSN {
	return common.LSN(i.currOffset)
}

func (i *MemoryLogIterator) Error() error {
	return i.err
}

func (i *MemoryLogIterator) Close() error {
	return nil
}
