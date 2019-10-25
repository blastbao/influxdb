package tsdb

import (
	"unsafe"

	"github.com/influxdata/influxdb/models"
)

const (
	// constants describing bit layout of id and type info
	seriesIDTypeFlag  = 1 << 63                   // a flag marking that the id contains type info 		// 最高位标识了是否存储类型信息
	seriesIDValueMask = 0xFFFFFFFF                // series ids numerically are 32 bits 				// 低32位存储SeriesID
	seriesIDTypeShift = 32                        // we put the type right after the value info 		// 高32位中的低16位存储类型信息
	seriesIDTypeMask  = 0xFF << seriesIDTypeShift // a mask for the type byte                           // 高32位中的低16位为类型信息位
	seriesIDSize      = 8                         // lint:ignore U1000 This const is used in a discarded compile-time type assertion.
)



// SeriesID is the type of a series id. It is logically a uint64, but encoded as a struct so
// that we gain more type checking when changing operations on it. The field is exported only
// so that tests that use reflection based comparisons still work; no one should use the field
// directly.
type SeriesID struct{
	ID uint64
}

// NewSeriesID constructs a series id from the raw value. It discards any type information.
func NewSeriesID(id uint64) SeriesID {
	return SeriesID{
		ID: id & seriesIDValueMask,
	}
}


// IsZero returns if the SeriesID is zero.
func (s SeriesID) IsZero() bool {
	return s.ID == 0
}

// ID returns the raw id for the SeriesID.
func (s SeriesID) RawID() uint64 {
	return s.ID
}

// WithType constructs a SeriesIDTyped with the given type.
func (s SeriesID) WithType(typ models.FieldType) SeriesIDTyped {
	// 注：
	// SeriesID 的最高位标识了 SeriesID 中是否存储了类型信息，如果最高位 s.ID | seriesIDTypeFlag 为 1，则
	// SeriesID 的高32位中的低16位 (uint64(typ&0xFF) << seriesIDTypeShift) 存储详细类型信息。
	return NewSeriesIDTyped(s.ID | seriesIDTypeFlag | (uint64(typ&0xFF) << seriesIDTypeShift))
}

// Greater returns if the SeriesID is greater than the passed in value.
func (s SeriesID) Greater(o SeriesID) bool {
	return s.ID > o.ID
}

// Less returns if the SeriesID is less than the passed in value.
func (s SeriesID) Less(o SeriesID) bool {
	return s.ID < o.ID
}


// SeriesIDType represents a series id with a type.
// It is logically a uint64, but encoded as a struct so that we gain more type checking when changing operations on it.
// The field is exported only so that tests that use reflection based comparisons still work;
// no one should use the field directly.
type SeriesIDTyped struct{
	ID uint64
}

// NewSeriesIDTyped constructs a typed series id from the raw values.
func NewSeriesIDTyped(id uint64) SeriesIDTyped {
	return SeriesIDTyped{
		ID: id,
	}
}

// IsZero returns if the SeriesIDTyped is zero. It ignores any type information.
func (s SeriesIDTyped) IsZero() bool {
	return s.ID&seriesIDValueMask == 0
}

// ID returns the raw id for the SeriesIDTyped.
func (s SeriesIDTyped) RawID() uint64 {
	return s.ID
}

// SeriesID constructs a SeriesID, discarding any type information.
func (s SeriesIDTyped) SeriesID() SeriesID {
	return NewSeriesID(s.ID)
}

// HasType returns if the id actually contains a type.
func (s SeriesIDTyped) HasType() bool {
	return s.ID&seriesIDTypeFlag > 0
}

// Type returns the associated type.
func (s SeriesIDTyped) Type() models.FieldType {
	//因为 SeriesID 的 seriesIDTypeMask 位存储了类型信息，这里移位后取出对应类型信息。
	return models.FieldType((s.ID & seriesIDTypeMask) >> seriesIDTypeShift)
}

type (
	// some static assertions that the SeriesIDSize matches the structs we defined.
	// if the values are not the same, at least one will be negative causing a compilation failure
	_ [seriesIDSize - unsafe.Sizeof(SeriesID{})]byte
	_ [unsafe.Sizeof(SeriesID{}) - seriesIDSize]byte

	_ [seriesIDSize - unsafe.Sizeof(SeriesIDTyped{})]byte
	_ [unsafe.Sizeof(SeriesIDTyped{}) - seriesIDSize]byte
)
