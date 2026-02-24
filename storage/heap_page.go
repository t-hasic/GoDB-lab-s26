package storage

import (
	"encoding/binary"
	"mit.edu/dsg/godb/common"
)

// HeapPage Layout:
// LSN (8) | RowSize (2) | NumSlots (2) |  NumUsed (2) | Padding (2) | allocation Bitmap | deleted Bitmap | rows
type HeapPage struct {
	*PageFrame
	allocationBitmapOffset int
	deletedBitmapOffset    int
	rowsOffset             int
	paddedBitmapSize       int
	rowSize                int
}

func (hp HeapPage) NumUsed() int {
	return int(binary.LittleEndian.Uint16(hp.Bytes[12:14]))
}

func (hp HeapPage) setNumUsed(numUsed int) {
	binary.LittleEndian.PutUint16(hp.Bytes[12:14], uint16(numUsed))
}

func (hp HeapPage) NumSlots() int {
	return int(binary.LittleEndian.Uint16(hp.Bytes[10:12]))
}

func (hp HeapPage) RowSize() int {
	return int(binary.LittleEndian.Uint16(hp.Bytes[8:10]))
}

func InitializeHeapPage(desc *RawTupleDesc, frame *PageFrame) {
	rowSize := desc.bytesPerRow
	headerSize := 16 // LSN(8) + RowSize(2) + NumSlots(2) + NumUsed(2) + Padding(2)

	// Compute numSlots iteratively: bitmap size depends on numSlots, so start
	// from an upper bound and reduce until everything fits in the page.
	numSlots := (common.PageSize - headerSize) / rowSize
	for {
		bitmapSize := (numSlots + 7) / 8
		paddedBitmapSize := ((bitmapSize + 7) / 8) * 8
		if headerSize+2*paddedBitmapSize+numSlots*rowSize <= common.PageSize {
			break
		}
		numSlots--
	}

	bitmapSize := (numSlots + 7) / 8
	paddedBitmapSize := ((bitmapSize + 7) / 8) * 8

	binary.LittleEndian.PutUint16(frame.Bytes[8:10], uint16(rowSize))
	binary.LittleEndian.PutUint16(frame.Bytes[10:12], uint16(numSlots))
	binary.LittleEndian.PutUint16(frame.Bytes[12:14], uint16(0))

	bitmapStart := headerSize
	for i := 0; i < 2*paddedBitmapSize; i++ {
		frame.Bytes[bitmapStart+i] = 0
	}
}

func (frame *PageFrame) AsHeapPage() HeapPage {
	numSlots := int(binary.LittleEndian.Uint16(frame.Bytes[10:12]))
	rowSize := int(binary.LittleEndian.Uint16(frame.Bytes[8:10]))

	bitmapSize := (numSlots + 7) / 8
	paddedBitmapSize := ((bitmapSize + 7) / 8) * 8

	allocationBitmapOffset := 16
	deletedBitmapOffset := allocationBitmapOffset + paddedBitmapSize
	rowsOffset := deletedBitmapOffset + paddedBitmapSize

	return HeapPage{
		PageFrame:              frame,
		allocationBitmapOffset: allocationBitmapOffset,
		deletedBitmapOffset:    deletedBitmapOffset,
		rowsOffset:             rowsOffset,
		paddedBitmapSize:       paddedBitmapSize,
		rowSize:                rowSize,
	}
}

func (hp HeapPage) FindFreeSlot() int {
	allocBitmap := AsBitmap(hp.PageFrame.Bytes[hp.allocationBitmapOffset:hp.deletedBitmapOffset], hp.NumSlots())
	for slot := 0; slot < hp.NumSlots(); slot++ {
		if !allocBitmap.LoadBit(slot) {
			return slot
		}
	}
	return -1
}

func (hp HeapPage) IsAllocated(rid common.RecordID) bool {
	allocBitmap := AsBitmap(hp.PageFrame.Bytes[hp.allocationBitmapOffset:hp.deletedBitmapOffset], hp.NumSlots())
	return allocBitmap.LoadBit(int(rid.Slot))
}

func (hp HeapPage) MarkAllocated(rid common.RecordID, allocated bool) {
	allocBitmap := AsBitmap(hp.PageFrame.Bytes[hp.allocationBitmapOffset:hp.deletedBitmapOffset], hp.NumSlots())
	wasAllocated := allocBitmap.SetBit(int(rid.Slot), allocated)
	if allocated && !wasAllocated {
		hp.setNumUsed(hp.NumUsed() + 1)
	} else if !allocated && wasAllocated {
		hp.setNumUsed(hp.NumUsed() - 1)
		// Clear the deleted bit when a slot is physically freed.
		deleteBitmap := AsBitmap(hp.PageFrame.Bytes[hp.deletedBitmapOffset:hp.rowsOffset], hp.NumSlots())
		deleteBitmap.SetBit(int(rid.Slot), false)
	}
}

func (hp HeapPage) IsDeleted(rid common.RecordID) bool {
	deleteBitmap := AsBitmap(hp.PageFrame.Bytes[hp.deletedBitmapOffset:hp.rowsOffset], hp.NumSlots())
	return deleteBitmap.LoadBit(int(rid.Slot))
}

func (hp HeapPage) MarkDeleted(rid common.RecordID, deleted bool) {
	deleteBitmap := AsBitmap(hp.PageFrame.Bytes[hp.deletedBitmapOffset:hp.rowsOffset], hp.NumSlots())
	deleteBitmap.SetBit(int(rid.Slot), deleted)
}

func (hp HeapPage) AccessTuple(rid common.RecordID) RawTuple {
	offset := hp.rowsOffset + hp.rowSize * int(rid.Slot)
	return hp.PageFrame.Bytes[offset : offset + hp.rowSize]
}
