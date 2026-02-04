package logging

import (
	"mit.edu/dsg/godb/common"
)

type LogRecordType uint16

const logBufferSize = 1 << 16

const (
	InvalidLogRecord LogRecordType = iota // So we can catch uninitialized values
	LogBeginTransaction
	LogCommit
	LogAbort
	LogInsert
	LogUpdate
	LogDelete
	LogBeginCheckpoint
	LogEndCheckpoint

	// CLRs (Compensation Log Records) record the UNDO of an operation.
	// They are written during transaction abort or recovery undo phases.
	// They ensure that if the system crashes during an undo, we don't try to undo the same operation twice.
	LogInsertCLR
	LogUpdateCLR
	LogDeleteCLR
)

func (t LogRecordType) String() string {
	switch t {
	case InvalidLogRecord:
		return "INVALID"
	case LogBeginTransaction:
		return "BEGIN TRANSACTION"
	case LogCommit:
		return "COMMIT"
	case LogAbort:
		return "ABORT"
	case LogInsert:
		return "INSERT"
	case LogUpdate:
		return "UPDATE"
	case LogDelete:
		return "DELETE"
	case LogBeginCheckpoint:
		return "BEGIN CHECKPOINT"
	case LogEndCheckpoint:
		return "END CHECKPOINT"
	case LogInsertCLR:
		return "INSERT CLR"
	case LogUpdateCLR:
		return "UPDATE CLR"
	case LogDeleteCLR:
		return "DELETE CLR"
	}
	return "UNKNOWN"
}

// LogManager is the interface for the system's Write-Ahead Log (WAL).
// It handles the append-only storage of log records and ensures durability guarantees.
type LogManager interface {
	// Append writes a log record to the log buffer.
	// It returns the LSN (Log Sequence Number) assigned to the record.
	// Note: This does not guarantee the record is on disk yet; use WaitUntilFlushed for that.
	Append(record LogRecord) (common.LSN, error)

	// WaitUntilFlushed blocks until the log record with the given LSN (and all prior records)
	// has been successfully written to stable storage (disk).
	WaitUntilFlushed(lsn common.LSN) error

	// Iterator returns a scanner to walk the log from a specific starting point.
	// This is primarily used during the Recovery process (Analysis, Redo, Undo).
	Iterator(startLSN common.LSN) (LogIterator, error)

	// FlushedUntil returns the highest LSN that is currently known to be on disk.
	FlushedUntil() common.LSN

	// Close cleans up file handles and ensures any pending buffers are flushed.
	Close() error
}

// LogIterator traverses log records sequentially.
type LogIterator interface {
	// Next advances the iterator to the next record.
	// It returns true if a record is available, or false if we hit EOF or an error.
	Next() bool

	// CurrentRecord returns the LogRecord at the current cursor.
	CurrentRecord() LogRecord

	// CurrentLSN returns the LSN of the current record.
	CurrentLSN() common.LSN

	// Error returns the first unexpected error that was encountered by the iterator.
	Error() error

	// Close releases resources associated with the iterator (e.g., file handles).
	Close() error
}
