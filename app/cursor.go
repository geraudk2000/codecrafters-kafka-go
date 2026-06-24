package main

import "encoding/binary"

// Cursor walks a byte slice, tracking its own read position.
// It knows nothing about Kafka — it just reads bytes and advances.
type Cursor struct {
	data []byte
	pos  int
}

func (c *Cursor) readInt8() int8 {
	v := int8(c.data[c.pos])
	c.pos++
	return v
}

func (c *Cursor) readInt16() int16 {
	v := int16(binary.BigEndian.Uint16(c.data[c.pos : c.pos+2]))
	c.pos += 2
	return v
}

func (c *Cursor) readInt32() int32 {
	v := int32(binary.BigEndian.Uint32(c.data[c.pos : c.pos+4]))
	c.pos += 4
	return v
}

func (c *Cursor) readInt64() int64 {
	v := int64(binary.BigEndian.Uint64(c.data[c.pos : c.pos+8]))
	c.pos += 8
	return v
}

func (c *Cursor) readBytes(n int) []byte {
	v := c.data[c.pos : c.pos+n]
	c.pos += n
	return v
}

func (c *Cursor) readUvarint() uint64 {
	var x uint64
	var s uint

	for {
		b := c.data[c.pos]
		c.pos++

		x |= uint64(b&0x7f) << s
		if b&0x80 == 0 {
			break
		}
		s += 7
	}

	return x
}

func zigzagDecode(n uint64) int64 {
	return int64((n >> 1) ^ uint64(-(int64(n) & 1)))
}

func (c *Cursor) readVarint() int32 {
	return int32(zigzagDecode(c.readUvarint()))
}

func (c *Cursor) readVarlong() int64 {
	return zigzagDecode(c.readUvarint())
}

// readCompactInt32Array reads a COMPACT_ARRAY of int32: a uvarint count
// stored as (realCount + 1), then that many int32 elements.
func (c *Cursor) readCompactInt32Array() []int32 {
	n := int(c.readUvarint()) - 1
	if n < 0 {
		return nil // null array
	}
	out := make([]int32, n)
	for i := 0; i < n; i++ {
		out[i] = c.readInt32()
	}
	return out
}

// skipCompactArray reads past a COMPACT_ARRAY whose elements we don't need.
// elemSize is the byte size of one element.
func (c *Cursor) skipCompactArray(elemSize int) {
	n := int(c.readUvarint()) - 1
	for i := 0; i < n; i++ {
		c.readBytes(elemSize)
	}
}
