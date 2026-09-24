// Package netcdf3 reads netCDF classic-format files (CDF-1 and CDF-2): the format
// NOAA's DSCOVR archive ships as, one day of 1-minute averages per file.
//
// Deliberately small. It reads the header, and the values of one-dimensional record
// variables -- every DSCOVR variable is (time). Anything else is reported as an error,
// never guessed at. Classic format is fully specified and simple enough that a
// dependency on an HDF5 binding, or on a general netCDF library, buys nothing here.
//
// Reference: https://docs.unidata.ucar.edu/netcdf-c/current/file_format_specifications.html
package netcdf3

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// External types.
const (
	Byte   = 1
	Char   = 2
	Short  = 3
	Int    = 4
	Float  = 5
	Double = 6
)

const (
	tagDimension = 0x0A
	tagVariable  = 0x0B
	tagAttribute = 0x0C
)

// Dim is one dimension. Length 0 marks the record (unlimited) dimension.
type Dim struct {
	Name   string
	Length int
}

// Var is one variable's header entry.
type Var struct {
	Name   string
	DimIDs []int
	Attrs  map[string]any
	Type   int
	vsize  int64
	begin  int64
}

// File is a parsed classic-format file held in memory.
type File struct {
	Version  int // 1 = classic, 2 = 64-bit offset
	NumRecs  int
	Dims     []Dim
	Attrs    map[string]any
	Vars     []*Var
	byName   map[string]*Var
	recSize  int64
	data     []byte
	recordID int // index of the record dimension, -1 if none
}

// Parse reads a classic-format file from memory.
func Parse(b []byte) (*File, error) {
	r := &reader{b: b}
	if len(b) < 8 || string(b[:3]) != "CDF" {
		return nil, errors.New("not a netCDF classic file (magic is not CDF)")
	}
	f := &File{Version: int(b[3]), data: b, byName: map[string]*Var{}, recordID: -1}
	if f.Version != 1 && f.Version != 2 {
		return nil, fmt.Errorf("unsupported netCDF classic version %d", f.Version)
	}
	r.off = 4
	nr := r.u32()
	if nr == 0xFFFFFFFF {
		return nil, errors.New("streaming numrecs is not supported")
	}
	f.NumRecs = int(nr)

	// Dimensions.
	tag, n := r.u32(), int(r.u32())
	if tag != 0 && tag != tagDimension {
		return nil, fmt.Errorf("expected dimension list, found tag 0x%x", tag)
	}
	for i := 0; i < n; i++ {
		d := Dim{Name: r.name(), Length: int(r.u32())}
		if d.Length == 0 {
			f.recordID = i
		}
		f.Dims = append(f.Dims, d)
	}

	var err error
	if f.Attrs, err = r.attrs(); err != nil {
		return nil, fmt.Errorf("global attributes: %w", err)
	}

	// Variables.
	tag, n = r.u32(), int(r.u32())
	if tag != 0 && tag != tagVariable {
		return nil, fmt.Errorf("expected variable list, found tag 0x%x", tag)
	}
	var recVars []*Var
	for i := 0; i < n; i++ {
		v := &Var{Name: r.name()}
		nd := int(r.u32())
		for j := 0; j < nd; j++ {
			id := int(r.u32())
			if id < 0 || id >= len(f.Dims) {
				return nil, fmt.Errorf("variable %s: dimension id %d out of range", v.Name, id)
			}
			v.DimIDs = append(v.DimIDs, id)
		}
		if v.Attrs, err = r.attrs(); err != nil {
			return nil, fmt.Errorf("variable %s attributes: %w", v.Name, err)
		}
		v.Type = int(r.u32())
		v.vsize = int64(r.u32())
		if f.Version == 1 {
			v.begin = int64(r.u32())
		} else {
			v.begin = int64(r.u64())
		}
		if r.err != nil {
			return nil, r.err
		}
		if len(v.DimIDs) > 0 && v.DimIDs[0] == f.recordID {
			recVars = append(recVars, v)
		}
		f.Vars = append(f.Vars, v)
		f.byName[v.Name] = v
	}
	if r.err != nil {
		return nil, r.err
	}

	// Record size: the sum of each record variable's per-record size, except that a
	// file with exactly ONE record variable does not pad it (the spec's special case).
	for _, v := range recVars {
		f.recSize += v.vsize
	}
	if len(recVars) == 1 {
		v := recVars[0]
		f.recSize = int64(typeSize(v.Type)) * f.innerLen(v)
	}
	return f, nil
}

// Var returns the named variable, or nil.
func (f *File) Var(name string) *Var { return f.byName[name] }

// innerLen is the number of values one record of v holds.
func (f *File) innerLen(v *Var) int64 {
	n := int64(1)
	for _, id := range v.DimIDs[1:] {
		n *= int64(f.Dims[id].Length)
	}
	return n
}

// Values returns a one-dimensional record variable as float64, one per record.
// Fill values are returned as they are stored; the caller decides what they mean.
func (f *File) Values(name string) ([]float64, error) {
	v := f.byName[name]
	if v == nil {
		return nil, fmt.Errorf("no variable %q", name)
	}
	if len(v.DimIDs) != 1 || v.DimIDs[0] != f.recordID {
		return nil, fmt.Errorf("variable %q is not a one-dimensional record variable", name)
	}
	sz := int64(typeSize(v.Type))
	if sz == 0 || v.Type == Char {
		return nil, fmt.Errorf("variable %q has non-numeric type %d", name, v.Type)
	}
	out := make([]float64, f.NumRecs)
	for i := 0; i < f.NumRecs; i++ {
		off := v.begin + int64(i)*f.recSize
		if off < 0 || off+sz > int64(len(f.data)) {
			return nil, fmt.Errorf("variable %q record %d lies outside the file (truncated?)", name, i)
		}
		p := f.data[off : off+sz]
		switch v.Type {
		case Byte:
			out[i] = float64(int8(p[0]))
		case Short:
			out[i] = float64(int16(binary.BigEndian.Uint16(p)))
		case Int:
			out[i] = float64(int32(binary.BigEndian.Uint32(p)))
		case Float:
			out[i] = float64(math.Float32frombits(binary.BigEndian.Uint32(p)))
		case Double:
			out[i] = math.Float64frombits(binary.BigEndian.Uint64(p))
		}
	}
	return out, nil
}

// Missing returns the value that marks a missing sample: _FillValue, or failing that
// missing_value. NOAA's DSCOVR files use missing_value (-99999.0) and no _FillValue,
// so reading only _FillValue would pass -99999 through as a measurement.
func (v *Var) Missing() (float64, bool) {
	for _, k := range []string{"_FillValue", "missing_value"} {
		if x, ok := v.Attrs[k].([]float64); ok && len(x) > 0 {
			return x[0], true
		}
	}
	return 0, false
}

func typeSize(t int) int {
	switch t {
	case Byte, Char:
		return 1
	case Short:
		return 2
	case Int, Float:
		return 4
	case Double:
		return 8
	}
	return 0
}

type reader struct {
	b   []byte
	off int
	err error
}

func (r *reader) need(n int) bool {
	if r.err != nil {
		return false
	}
	if r.off+n > len(r.b) {
		r.err = errors.New("header runs past end of file")
		return false
	}
	return true
}

func (r *reader) u32() uint32 {
	if !r.need(4) {
		return 0
	}
	v := binary.BigEndian.Uint32(r.b[r.off:])
	r.off += 4
	return v
}

func (r *reader) u64() uint64 {
	if !r.need(8) {
		return 0
	}
	v := binary.BigEndian.Uint64(r.b[r.off:])
	r.off += 8
	return v
}

func pad4(n int) int { return (n + 3) &^ 3 }

func (r *reader) name() string {
	n := int(r.u32())
	if !r.need(pad4(n)) {
		return ""
	}
	s := string(r.b[r.off : r.off+n])
	r.off += pad4(n)
	return s
}

// attrs reads an attribute list. Numeric values come back as []float64, text as string.
func (r *reader) attrs() (map[string]any, error) {
	out := map[string]any{}
	tag, n := r.u32(), int(r.u32())
	if tag != 0 && tag != tagAttribute {
		return nil, fmt.Errorf("expected attribute list, found tag 0x%x", tag)
	}
	for i := 0; i < n && r.err == nil; i++ {
		name := r.name()
		t := int(r.u32())
		cnt := int(r.u32())
		sz := typeSize(t)
		if sz == 0 {
			return nil, fmt.Errorf("attribute %s: unknown type %d", name, t)
		}
		raw := cnt * sz
		if !r.need(pad4(raw)) {
			break
		}
		p := r.b[r.off : r.off+raw]
		r.off += pad4(raw)
		if t == Char {
			out[name] = string(p)
			continue
		}
		vals := make([]float64, cnt)
		for j := 0; j < cnt; j++ {
			q := p[j*sz:]
			switch t {
			case Byte:
				vals[j] = float64(int8(q[0]))
			case Short:
				vals[j] = float64(int16(binary.BigEndian.Uint16(q)))
			case Int:
				vals[j] = float64(int32(binary.BigEndian.Uint32(q)))
			case Float:
				vals[j] = float64(math.Float32frombits(binary.BigEndian.Uint32(q)))
			case Double:
				vals[j] = math.Float64frombits(binary.BigEndian.Uint64(q))
			}
		}
		out[name] = vals
	}
	return out, r.err
}
