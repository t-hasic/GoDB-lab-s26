package storage

import (
	"mit.edu/dsg/godb/common"
	"github.com/puzpuzpuz/xsync/v4"
	"sync"
	"fmt"
	"log"
	"sync/atomic"
)

// BufferPool manages the reading and writing of database pages between the DiskFileManager and memory.
// It acts as a central cache to keep "hot" pages in memory with fixed capacity and selectively evicts
// pages to disk when the pool becomes full. Users will need to coordinate concurrent access to pages
// using page-level latches and metadata (which you should define in page.go). All methods
// must be thread-safe, as multiple threads will request the same or different pages concurrently.
// To get full credit, you likely need to do better than coarse-grained latching (i.e., a global latch for the entire
// BufferPool instance).
type BufferPool struct {
	// add more fields here...
	frames []PageFrame
	refBits []atomic.Bool  // separated from PageFrame for cache locality
	pageTable *xsync.MapOf[common.PageID, *PageFrame]
	storageManager DBFileManager
	logManager LogManager
	bufferPoolMutex sync.Mutex
	clockHand int
}

// NewBufferPool creates a new BufferPool with a fixed capacity defined by numPages. It requires a
// storageManager to handle the underlying disk I/O operations.
//
// Hint: You will need to worry about logManager until Lab 3
func NewBufferPool(numPages int, storageManager DBFileManager, logManager LogManager) *BufferPool {
	bp := &BufferPool{
		frames: make([]PageFrame, numPages),
		pageTable: xsync.NewMapOf[common.PageID, *PageFrame](),
		storageManager: storageManager,
		logManager: logManager,
		bufferPoolMutex: sync.Mutex{},
		clockHand: 0,
	}
	for i := range bp.frames {
		bp.frames[i].index = i
	}
	bp.refBits = make([]atomic.Bool, numPages)
	return bp
}

// StorageManager returns the underlying disk manager.
func (bp *BufferPool) StorageManager() DBFileManager {
	return bp.storageManager
}

// GetPage retrieves a page from the buffer pool, ensuring it is pinned (i.e. prevented from eviction until
// unpinned) and ready for use. If the page is already in the pool, the cached bytes are returned. If the page is not
// present, the method must first make space by selecting a victim frame to evict
// (potentially writing it to disk if dirty), and then read the requested page from disk into that frame.
func (bp *BufferPool) GetPage(pageID common.PageID) (*PageFrame, error) {

	frame, err := bp.tryGetPageFromTable(pageID)
	if err == nil {
		// Cache hit
		return frame, nil
	}

	// Cache miss
	// Lock buffer pool because we are editing pages
	bp.bufferPoolMutex.Lock()
	// Try again to get the page (may now be in the table)
	frame, err = bp.tryGetPageFromTable(pageID)
	if err == nil {
		bp.bufferPoolMutex.Unlock()
		return frame, nil
	}

	// Find a victim
	victim, err := bp.findVictim()
	if err == nil {
		oldPageID := victim.pageID
		wasDirty := victim.dirty
		wasValid := victim.state == VALID

		// replace with new page
		victim.pageID = pageID
		victim.state = LOADING
		victim.pinCount = 0
		victim.dirty = false
		victim.loadDone = make(chan struct{})
		victim.metadataMutex.Unlock()

		bp.pageTable.Store(pageID, victim)
		bp.bufferPoolMutex.Unlock()

		// write back victim to disk if dirty
		if wasDirty && wasValid {
			dbFile, err := bp.storageManager.GetDBFile(oldPageID.Oid)
			if err != nil {
				log.Printf("failed to get db file for page %v: %v", oldPageID, err)
			} else {
				err = dbFile.WritePage(int(oldPageID.PageNum), victim.Bytes[:])
				if err != nil {
					log.Printf("failed to write dirty page %v to disk: %v", oldPageID, err)
				}
			}
		}
		// Evict victim from buffer pool
		bp.pageTable.Delete(oldPageID)

		// read new page from disk
		victim.PageLatch.Lock()
		dbFile, err := bp.storageManager.GetDBFile(pageID.Oid)
		if err != nil {
			log.Printf("failed to get db file for page %v: %v", pageID, err)
		} else {
			err = dbFile.ReadPage(int(pageID.PageNum), victim.Bytes[:])
			if err != nil {
				log.Printf("failed to read page %v from disk: %v", pageID, err)
			}
		}
		victim.PageLatch.Unlock()
		victim.metadataMutex.Lock()
		victim.state = VALID
		victim.pinCount++
		bp.refBits[victim.index].Store(false)
		close(victim.loadDone)
		victim.metadataMutex.Unlock()
		return victim, nil
	}
	return nil, fmt.Errorf("failed to find victim")
}

func (bp *BufferPool) tryGetPageFromTable(pageID common.PageID) (*PageFrame, error) {
	for {
		if frame, ok := bp.pageTable.Load(pageID); ok {
			// Protect metadata	
			frame.metadataMutex.Lock()
			// Check the state of the frame by case
			switch frame.state {
				case VALID:
					// check if pageID still matches (could have been evicted before we locked the metadata)	
					if frame.pageID == pageID {
						frame.pinCount++
						bp.refBits[frame.index].Store(true)
						frame.metadataMutex.Unlock()
						return frame, nil
					}
					frame.metadataMutex.Unlock() // proceed to cache miss, page was evicted in meantime
				case LOADING:
					// wait for other goroutine to finish loading page from disk
					frame.metadataMutex.Unlock()
					<-frame.loadDone
					continue // let the waiting goroutines fight to grab the frame
				case INVALID:
					// proceed to cache miss
					frame.metadataMutex.Unlock()
					return nil, fmt.Errorf("page not found")
			}
		} else {
			return nil, fmt.Errorf("page not found")
		}
	}
}

func (bp *BufferPool) findVictim() (*PageFrame, error) {
    n := len(bp.frames)
    maxSweep := min(n, 1000)
    for i := 0; ; i++ {
        frame := &bp.frames[bp.clockHand]
        bp.clockHand = (bp.clockHand + 1) % n

        if i < maxSweep && bp.refBits[frame.index].CompareAndSwap(true, false) {
            continue
        }

        frame.metadataMutex.Lock()
        if frame.pinCount > 0 || frame.state == LOADING {
            frame.metadataMutex.Unlock()
            continue
        }

        if i < maxSweep && bp.refBits[frame.index].CompareAndSwap(true, false) {
            frame.metadataMutex.Unlock()
            continue
        }

        return frame, nil
    }
}

// UnpinPage indicates that the caller is done using a page. It unpins the page, making the page potentially evictable
// if no other thread is accessing it. If the setDirty flag is true, the page is marked as modified, ensuring
// it will be written back to disk before eviction.
func (bp *BufferPool) UnpinPage(frame *PageFrame, setDirty bool) {
	frame.metadataMutex.Lock()
	frame.pinCount--
	if setDirty {
		frame.dirty = true
	}
    frame.metadataMutex.Unlock()
}

// FlushAllPages flushes all dirty pages to disk that have an LSN less than `flushedUntil`, regardless of pins.
// This is typically called during a Checkpoint or Shutdown to ensure durability, but also useful for tests
func (bp *BufferPool) FlushAllPages() error {
	for i := 0; i < len(bp.frames); i++ {
		frame := &bp.frames[i]
		frame.metadataMutex.Lock()
		frame.PageLatch.Lock()
		if frame.dirty && frame.LSN() < bp.logManager.FlushedUntil() {
			dbFile, err := bp.storageManager.GetDBFile(frame.pageID.Oid)
			if err != nil {
				log.Printf("failed to get db file for page %v: %v", frame.pageID, err)
			} else {
				err = dbFile.WritePage(int(frame.pageID.PageNum), frame.Bytes[:])
				if err != nil {
					log.Printf("failed to write dirty page %v to disk: %v", frame.pageID, err)
				}
			}
			if err != nil {
				return err
			}
			frame.dirty = false
		}
		frame.PageLatch.Unlock()
		frame.metadataMutex.Unlock()
	}
	return nil;
}

// GetDirtyPageTableSnapshot returns a map of all currently dirty pages and their RecoveryLSN.
// This is used by the Recovery Manager (ARIES) during the Analysis phase to reconstruct the
// state of the database.
//
// Hint: You do not need to worry about this function until lab 4
func (bp *BufferPool) GetDirtyPageTableSnapshot() map[common.PageID]LSN {
	// You will not need to implement this until lab4
	panic("unimplemented")
}