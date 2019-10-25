package tsdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"

	"github.com/influxdata/influxdb/pkg/mmap"
)

const (
	SeriesSegmentVersion = 1
	SeriesSegmentMagic   = "SSEG"

	SeriesSegmentHeaderSize = 4 + 1 // magic + version
)

// Series entry constants.
const (
	SeriesEntryFlagSize   = 1
	SeriesEntryHeaderSize = 1 + 8 // flag + id

	SeriesEntryInsertFlag    = 0x01  //inserted
	SeriesEntryTombstoneFlag = 0x02  //deleted
)

var (
	ErrInvalidSeriesSegment        = errors.New("invalid series segment")
	ErrInvalidSeriesSegmentVersion = errors.New("invalid series segment version")
	ErrSeriesSegmentNotWritable    = errors.New("series segment not writable")
)

// SeriesSegment represents a log of series entries.
type SeriesSegment struct {
	id   uint16        // SegmentID
	path string		   //

	data []byte        // mmap file
	file *os.File      // write file handle
	w    *bufio.Writer // bufferred file handle
	size uint32        // current file size
}

// NewSeriesSegment returns a new instance of SeriesSegment.
func NewSeriesSegment(id uint16, path string) *SeriesSegment {
	return &SeriesSegment{
		id:   id,
		path: path,
	}
}

// CreateSeriesSegment generates an empty segment file at path.
func CreateSeriesSegment(id uint16, path string) (*SeriesSegment, error) {
	// Generate segment in temp location.
	f, err := os.Create(path + ".initializing")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Write header to file and close.
	hdr := NewSeriesSegmentHeader()
	if _, err := hdr.WriteTo(f); err != nil {
		return nil, err
	} else if err := f.Truncate(int64(SeriesSegmentSize(id))); err != nil {
		return nil, err
	} else if err := f.Close(); err != nil {
		return nil, err
	}

	// Swap with target path.
	if err := os.Rename(f.Name(), path); err != nil {
		return nil, err
	}

	// Open segment at new location.
	segment := NewSeriesSegment(id, path)
	if err := segment.Open(); err != nil {
		return nil, err
	}
	return segment, nil
}

// Open memory maps the data file at the file's path.
func (s *SeriesSegment) Open() error {
	if err := func() (err error) {
		// Memory map file data.
		if s.data, err = mmap.Map(s.path, int64(SeriesSegmentSize(s.id))); err != nil {
			return err
		}

		// Read header.
		hdr, err := ReadSeriesSegmentHeader(s.data)
		if err != nil {
			return err
		} else if hdr.Version != SeriesSegmentVersion {
			return ErrInvalidSeriesSegmentVersion
		}

		return nil
	}(); err != nil {
		s.Close()
		return err
	}

	return nil
}

// InitForWrite initializes a write handle for the segment.
// This is only used for the last segment in the series file (AOF policy).
func (s *SeriesSegment) InitForWrite() (err error) {

	//1. traverse all SeriesEntrys of s.data[] in order to get the current wirte offset.
	for s.size = uint32(SeriesSegmentHeaderSize); s.size < uint32(len(s.data)); {
		flag, _, _, sz := ReadSeriesEntry(s.data[s.size:])
		if !IsValidSeriesEntryFlag(flag) {
			break
		}
		s.size += uint32(sz)
	}

	//2. open file handler for writing & seek to current wirte offset.
	if s.file, err = os.OpenFile(s.path, os.O_WRONLY|os.O_CREATE, 0666); err != nil {
		return err
	} else if _, err := s.file.Seek(int64(s.size), io.SeekStart); err != nil {
		return err
	}

	//3. wrap file handler with a buffer which has at least 32*1024 bytes.
	s.w = bufio.NewWriterSize(s.file, 32*1024)

	return nil
}

// Close unmaps the segment.
func (s *SeriesSegment) Close() (err error) {
	if e := s.CloseForWrite(); e != nil && err == nil {
		err = e
	}

	if s.data != nil {
		if e := mmap.Unmap(s.data); e != nil && err == nil {
			err = e
		}
		s.data = nil
	}

	return err
}

func (s *SeriesSegment) CloseForWrite() (err error) {
	if s.w != nil {
		if e := s.w.Flush(); e != nil && err == nil {
			err = e
		}
		s.w = nil
	}

	if s.file != nil {
		if e := s.file.Close(); e != nil && err == nil {
			err = e
		}
		s.file = nil
	}
	return err
}

// Data returns the raw data.
func (s *SeriesSegment) Data() []byte { return s.data }

// ID returns the id the segment was initialized with.
func (s *SeriesSegment) ID() uint16 { return s.id }

// Size returns the size of the data in the segment.
// This is only populated once InitForWrite() is called.
func (s *SeriesSegment) Size() int64 { return int64(s.size) }

// Slice returns a byte slice starting at pos.
func (s *SeriesSegment) Slice(pos uint32) []byte { return s.data[pos:] }


// WriteLogEntry writes entry data into the segment.
// Returns the offset of the beginning of the entry.
func (s *SeriesSegment) WriteLogEntry(data []byte) (offset int64, err error) {
	if !s.CanWrite(data) {
		return 0, ErrSeriesSegmentNotWritable
	}

	offset = JoinSeriesOffset(s.id, s.size)
	if _, err := s.w.Write(data); err != nil {
		return 0, err
	}
	s.size += uint32(len(data))
	return offset, nil
}

// CanWrite returns true if segment has space to write entry data.
func (s *SeriesSegment) CanWrite(data []byte) bool {
	return s.w != nil && s.size+uint32(len(data)) <= SeriesSegmentSize(s.id)
}

// Flush flushes the buffer to disk.
func (s *SeriesSegment) Flush() error {
	if s.w == nil {
		return nil
	}
	return s.w.Flush()
}

// AppendSeriesIDs appends all the segments ids to a slice.
// Returns the new slice.
func (s *SeriesSegment) AppendSeriesIDs(a []SeriesID) []SeriesID {
	s.ForEachEntry(
		func(flag uint8, id SeriesIDTyped, _ int64, _ []byte) error {
			if flag == SeriesEntryInsertFlag {
				a = append(a, id.SeriesID())
			}
			return nil
		},
	)
	return a
}

// MaxSeriesID returns the highest series id in the segment.
func (s *SeriesSegment) MaxSeriesID() SeriesID {
	var max SeriesID
	s.ForEachEntry(
		func(flag uint8, id SeriesIDTyped, _ int64, _ []byte) error {
			untypedID := id.SeriesID()
			if flag == SeriesEntryInsertFlag && untypedID.Greater(max) {
				max = untypedID
			}
			return nil
		},
	)
	return max
}

// ForEachEntry executes fn for every entry in the segment.
func (s *SeriesSegment) ForEachEntry(fn func(flag uint8, id SeriesIDTyped, offset int64, key []byte) error) error {

	for pos := uint32(SeriesSegmentHeaderSize); pos < uint32(len(s.data)); {

		// read entry
		flag, id, key, sz := ReadSeriesEntry(s.data[pos:])

		// check if flag is valid.
		if !IsValidSeriesEntryFlag(flag) {
			break
		}

		// offset is a interger with higher 2-byte segmentID and lower 4-byte pos.
		offset := JoinSeriesOffset(s.id, pos)


		if err := fn(flag, id, offset, key); err != nil {
			return err
		}

		pos += uint32(sz)
	}

	return nil
}

// Clone returns a copy of the segment. Excludes the write handler, if set.
func (s *SeriesSegment) Clone() *SeriesSegment {
	return &SeriesSegment{
		id:   s.id,
		path: s.path,
		data: s.data,
		size: s.size,
	}
}

// CloneSeriesSegments returns a copy of a slice of segments.
func CloneSeriesSegments(a []*SeriesSegment) []*SeriesSegment {
	other := make([]*SeriesSegment, len(a))
	for i := range a {
		other[i] = a[i].Clone()
	}
	return other
}

// FindSegment returns a segment by id.
func FindSegment(a []*SeriesSegment, id uint16) *SeriesSegment {
	for _, segment := range a {
		if segment.id == id {
			return segment
		}
	}
	return nil
}

// ReadSeriesKeyFromSegments returns a series key from an offset within a set of segments.
func ReadSeriesKeyFromSegments(a []*SeriesSegment, offset int64) []byte {
	//1. 从 offset 中分离出 segmentID 和 pos
	segmentID, pos := SplitSeriesOffset(offset)
	//2. 根据 segmentID 定位到 segment
	segment := FindSegment(a, segmentID)
	if segment == nil {
		return nil
	}
	//3. 根据 pos 定位到数据 buf
	buf := segment.Slice(pos)
	//4. 从 buf 处读取一个 SeriesKey 并返回
	key, _ := ReadSeriesKey(buf)
	return key
}

// JoinSeriesOffset returns an offset that combines the 2-byte segmentID and 4-byte pos.
// 通过segment id可以知道写入了哪个segment文件，通过segment size可知道写了segment文件的什么位置。
func JoinSeriesOffset(segmentID uint16, pos uint32) int64 {
	return (int64(segmentID) << 32) | int64(pos)
}

// SplitSeriesOffset splits a offset into its 2-byte segmentID and 4-byte pos parts.
func SplitSeriesOffset(offset int64) (segmentID uint16, pos uint32) {
	return uint16((offset >> 32) & 0xFFFF), uint32(offset & 0xFFFFFFFF)
}

// IsValidSeriesSegmentFilename returns true if filename is a 4-character lowercase hexidecimal number.
func IsValidSeriesSegmentFilename(filename string) bool {
	return seriesSegmentFilenameRegex.MatchString(filename)
}

// ParseSeriesSegmentFilename returns the id represented by the hexidecimal filename.
func ParseSeriesSegmentFilename(filename string) (uint16, error) {
	i, err := strconv.ParseUint(filename, 16, 32)
	return uint16(i), err
}

var seriesSegmentFilenameRegex = regexp.MustCompile(`^[0-9a-f]{4}$`)

// SeriesSegmentSize returns the maximum size of the segment.
// The size goes up by powers of 2 starting from 4MB and reaching 256MB.
func SeriesSegmentSize(id uint16) uint32 {
	const min = 22 // 4MB
	const max = 28 // 256MB

	shift := id + min
	if shift >= max {
		shift = max
	}
	return 1 << shift
}

// SeriesSegmentHeader represents the header of a series segment.
type SeriesSegmentHeader struct {
	Version uint8
}

// NewSeriesSegmentHeader returns a new instance of SeriesSegmentHeader.
func NewSeriesSegmentHeader() SeriesSegmentHeader {
	return SeriesSegmentHeader{Version: SeriesSegmentVersion}
}

// ReadSeriesSegmentHeader returns the header from data.
func ReadSeriesSegmentHeader(data []byte) (hdr SeriesSegmentHeader, err error) {
	r := bytes.NewReader(data)

	// Read magic number.
	magic := make([]byte, len(SeriesSegmentMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return hdr, err
	} else if !bytes.Equal([]byte(SeriesSegmentMagic), magic) {
		return hdr, ErrInvalidSeriesSegment
	}

	// Read version.
	if err := binary.Read(r, binary.BigEndian, &hdr.Version); err != nil {
		return hdr, err
	}

	return hdr, nil
}

// WriteTo writes the header to w.
func (hdr *SeriesSegmentHeader) WriteTo(w io.Writer) (n int64, err error) {
	var buf bytes.Buffer
	buf.WriteString(SeriesSegmentMagic)
	binary.Write(&buf, binary.BigEndian, hdr.Version)
	return buf.WriteTo(w)
}


// Read&Parse SeriesEntry
func ReadSeriesEntry(data []byte) (flag uint8, id SeriesIDTyped, key []byte, sz int64) {
	// If flag byte is zero then no more entries exist.
	flag, data = uint8(data[0]), data[1:]
	if !IsValidSeriesEntryFlag(flag) {
		return 0, SeriesIDTyped{}, nil, 1
	}

	// Read SeriesID(8B) and reminder data[8:]
	id, data = NewSeriesIDTyped(binary.BigEndian.Uint64(data)), data[8:]
	switch flag {
	case SeriesEntryInsertFlag:
		key, _ = ReadSeriesKey(data)
	}

	// SeriesEntry(Flag(1B), SeriesID(8B), SeriesKey(varint), Size=1+8+len(key) )
	return flag, id, key, int64(SeriesEntryHeaderSize + len(key))
}



func AppendSeriesEntry(dst []byte, flag uint8, id SeriesIDTyped, key []byte) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, id.RawID())

	dst = append(dst, flag)      //1. flag 1B
	dst = append(dst, buf...)    //2. SeriesID 8B

	switch flag {
	case SeriesEntryInsertFlag:
		dst = append(dst, key...)    //3. SeriesKey
	case SeriesEntryTombstoneFlag:
	default:
		panic(fmt.Sprintf("unreachable: invalid flag: %d", flag))
	}
	return dst
}

// IsValidSeriesEntryFlag returns true if flag is valid.
func IsValidSeriesEntryFlag(flag byte) bool {
	switch flag {
	case SeriesEntryInsertFlag, SeriesEntryTombstoneFlag:
		return true
	default:
		return false
	}
}
