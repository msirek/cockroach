// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build !fast_int_set_small && !fast_int_set_large
// +build !fast_int_set_small,!fast_int_set_large

package util

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"golang.org/x/tools/container/intsets"
)

// FastIntSet keeps track of a set of integers. It does not perform any
// allocations when the values are small. It is not thread-safe.
type FastIntSet struct {
	small bitmap
	large *intsets.Sparse
}

type FastIntSet2 struct {
	small bitmap
	large largeBitmap
}

// We maintain a bitmap for small element values (specifically 0 to
// smallCutoff-1). When this bitmap is sufficient, we avoid allocating the
// `Sparse` set.  Even when we have to allocate the `Sparse` set, we still
// maintain the bitmap as it can provide a fast path for certain operations.
// Note: this can be set to a smaller value, e.g. for testing.
const smallCutoff = 128

// bitmap implements a bitmap of size smallCutoff.
type bitmap struct {
	// We don't use an array because that makes Go always keep the struct on the
	// stack (see https://github.com/golang/go/issues/24416).
	lo, hi uint64
}

var emptyBitmap bitmap

func (b *bitmap) copyFrom(other *bitmap) {
	b.lo = other.lo
	b.hi = other.hi
}

func (b *bitmap) isEmpty() bool {
	return *b == emptyBitmap
}

// bitmapMap implements a map of bitmaps for ints.
type bitmapMap map[int]*bitmap

type bitmapKeys []int

var _ sort.Interface = (bitmapKeys)(nil)

func (b bitmapKeys) Len() int           { return len(b) }
func (b bitmapKeys) Less(i, j int) bool { return b[i] < b[j] }
func (b bitmapKeys) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

// largeBitmap implements a large bitmaps that can handle ints outside the range
// [0, smallCutoff).
type largeBitmap struct {
	bmap       bitmapMap
	sortedKeys bitmapKeys
}

func (s *largeBitmap) String() string {
	stringBuilder := &strings.Builder{}
	stringBuilder.WriteString("\n      bmap: {\n")
	numEntries := len(s.bmap)
	i := 1
	for key, val := range s.bmap {
		stringBuilder.WriteString(fmt.Sprintf("              { key: %d    val: %v }", key, *val))
		if i != numEntries {
			stringBuilder.WriteString(fmt.Sprintf(",\n"))
		}
		i++
	}
	stringBuilder.WriteString(fmt.Sprintf("\n      }\n      sortedKeys: { %v }", s.sortedKeys))
	return stringBuilder.String()
}

func (b *largeBitmap) isEmpty() bool {
	return len(b.bmap) == 0
}

func (b *largeBitmap) Equals(other *largeBitmap) bool {
	if len(b.sortedKeys) != len(other.sortedKeys) {
		return false
	}
	for i, key := range b.sortedKeys {
		if key != other.sortedKeys[i] {
			return false
		}
		var bmap, otherBmap *bitmap
		var ok bool
		if bmap, ok = b.bmap[key]; !ok {
			panic(errors.AssertionFailedf("unable to find bitmap for key in sortedKeys"))
		}
		if otherBmap, ok = other.bmap[key]; !ok {
			panic(errors.AssertionFailedf("unable to find bitmap for key in other.sortedKeys"))
		}
		if *bmap != *otherBmap {
			return false
		}
	}
	return true
}

func (b *largeBitmap) copyFrom(other *largeBitmap) {
	if other.bmap != nil {
		b.sortedKeys = make(bitmapKeys, 0, len(other.sortedKeys))
		b.sortedKeys = append(b.sortedKeys, other.sortedKeys...)
		b.bmap = make(bitmapMap, len(other.bmap))
		for otherKey, otherMap := range other.bmap {
			newMap := &bitmap{}
			newMap.copyFrom(otherMap)
			b.bmap[otherKey] = newMap
			b.insert(otherKey)
		}
	} else {
		b.bmap = nil
		b.sortedKeys = nil
	}
}

func (b *largeBitmap) Min() int {
	if b.isEmpty() || len(b.sortedKeys) == 0 {
		panic(errors.AssertionFailedf("no min in largeBitmap"))
	}
	minKey := b.sortedKeys[0]
	if bmap, ok := b.bmap[minKey]; ok {
		minVal := bmap.Min()
		return intFromKeyVal(minKey, minVal)
	}
	panic(errors.AssertionFailedf("no min in largeBitmap"))
	return 0
}

func (b *largeBitmap) Max() int {
	if b.isEmpty() || len(b.sortedKeys) == 0 {
		panic(errors.AssertionFailedf("no max in largeBitmap"))
	}
	maxKey := b.sortedKeys[len(b.sortedKeys)-1]
	if bmap, ok := b.bmap[maxKey]; ok {
		maxVal := bmap.Max()
		return intFromKeyVal(maxKey, maxVal)
	}
	panic(errors.AssertionFailedf("no max in largeBitmap"))
	return 0
}

// insert inserts `key` into the sortedKeys slice.
// If val already exists in the slice, do nothing.
func (b *largeBitmap) insert(key int) {
	i := sort.SearchInts(b.sortedKeys, key)
	if i < len(b.sortedKeys) && key == b.sortedKeys[i] {
		return
	}
	if b.sortedKeys == nil {
		b.sortedKeys = make(bitmapKeys, 0, 2)
	}
	if i == len(b.sortedKeys) {
		b.sortedKeys = append(b.sortedKeys, key)
	} else {
		b.sortedKeys = append(b.sortedKeys[:i+1], b.sortedKeys[i:]...)
		b.sortedKeys[i] = key
	}
}

// delete deletes `key`` from the sortedKeys slice.
// If val does not exist in the slice, do nothing.
func (b *largeBitmap) delete(key int) {
	i := sort.SearchInts(b.sortedKeys, key)
	if i >= len(b.sortedKeys) || key != b.sortedKeys[i] {
		return
	}
	b.sortedKeys = append(b.sortedKeys[:i], b.sortedKeys[i+1:]...)
}

// MakeFastIntSet returns a set initialized with the given values.
func MakeFastIntSet(vals ...int) FastIntSet {
	var res FastIntSet
	for _, v := range vals {
		res.Add(v)
	}
	return res
}

func MakeFastIntSet2(vals ...int) FastIntSet2 {
	var res FastIntSet2
	for _, v := range vals {
		res.Add(v)
	}
	return res
}

func (s *FastIntSet) toLarge() *intsets.Sparse {
	if s.large != nil {
		return s.large
	}
	large := new(intsets.Sparse)
	for i, ok := s.Next(0); ok; i, ok = s.Next(i + 1) {
		large.Insert(i)
	}
	return large
}

// msirek-temp
func (s *FastIntSet2) toLarge() *largeBitmap {
	if s.large.bmap != nil {
		return &s.large
	}
	s.large.bmap = make(bitmapMap, 2)
	// msirek-temp:  Maybe we can just point to the small bitmap.
	bitMap := &bitmap{}
	if !s.small.isEmpty() {
		s.large.bmap[0] = bitMap
		bitMap.copyFrom(&s.small)
		s.large.insert(0)
	}
	return &s.large
}

// fitsInSmall returns whether all elements in this set are between 0 and
// smallCutoff.
func (s *FastIntSet) fitsInSmall() bool {
	if s.large == nil {
		return true
	}
	// It is possible that we have a large set allocated but all elements still
	// fit the cutoff. This can happen if the set used to contain other elements.
	return s.large.Min() >= 0 && s.large.Max() < smallCutoff
}

func (s *FastIntSet2) fitsInSmall() bool {
	if s.large.bmap == nil {
		return true
	}
	if s.large.isEmpty() {
		return true
	}
	// It is possible that we have a large set allocated but all elements still
	// fit the cutoff. This can happen if the set used to contain other elements.
	return s.large.Min() >= 0 && s.large.Max() < smallCutoff
}

// Add adds a value to the set. No-op if the value is already in the set. If the
// large set is not nil and the value is within the range [0, 63], the value is
// added to both the large and small sets.
func (s *FastIntSet) Add(i int) {
	withinSmallBounds := i >= 0 && i < smallCutoff
	largeBounds := i > 10000
	if largeBounds {
		panic("too large!")
		//fmt.Println("too large!") // msirek-temp
	}
	if withinSmallBounds {
		s.small.Set(i)
	}
	if !withinSmallBounds && s.large == nil {
		s.large = s.toLarge()
	}
	if s.large != nil {
		s.large.Insert(i)
	}
}

func isSmall(i int) bool {
	return i >= 0 && i < smallCutoff
}

// keyVal finds the key into the largeBitmap.bmap for a given integer, i, and
// the associated bit, val, to check or set in that bitmap.
func keyVal(i int) (key int, val int) {
	//if isSmall(i) {
	//	panic("invalid index when computing KV for largeBitmap")
	//} // msirek-temp
	if i < 0 {
		key = ((i + 1) / smallCutoff) - 1
		// Map -128 to 0 and -1 to 127, so the positive numbers we're mapping to
		// have the same ordering as their corresponding original negative numbers.
		val = smallCutoff + ((i + 1) % smallCutoff) - 1
	} else {
		key = (i / smallCutoff)
		val = i % smallCutoff
	}
	return key, val
}

// intFromKeyVal maps from a (key, val) pair in a largeBitmap.bmap, where `val`
// is a given bit in the bitmap with key `key`.
func intFromKeyVal(key int, val int) int {
	if !isSmall(val) {
		panic(errors.AssertionFailedf(fmt.Sprintf("invalid val: %d passed to intFromKeyVal", val)))
	}
	return key*smallCutoff + val
}

// getBitmap returns the bitmap associated with a given integer, if it exists.
func (b *largeBitmap) getBitmap(i int) (bitMap *bitmap, ok bool) {
	key, _ := keyVal(i)
	bitMap, ok = b.bmap[key]
	return bitMap, ok
}

func (b *largeBitmap) contains(i int) bool {
	key, val := keyVal(i)
	if bitMap, ok := b.bmap[key]; ok {
		return bitMap.IsSet(val)
	}
	return false
}

func (s *FastIntSet2) getOrMakeBitmap(i int) (bitMap *bitmap, val int) {
	key, val := keyVal(i)
	var ok bool
	if bitMap, ok = s.large.bmap[key]; !ok {
		bitMap = &bitmap{}
		s.large.bmap[key] = bitMap
		s.large.insert(key)
	}
	return bitMap, val
}

func (b *largeBitmap) remove(i int) {
	key, val := keyVal(i)
	var bitMap *bitmap
	var ok bool
	if bitMap, ok = b.getBitmap(i); ok {
		bitMap.Unset(val)
		if bitMap.isEmpty() {
			delete(b.bmap, key)
			b.delete(key)
		}
	}
}

func (s *FastIntSet2) Add(i int) {
	withinSmallBounds := i >= 0 && i < smallCutoff

	if withinSmallBounds {
		s.small.Set(i)
		if s.large.bmap == nil {
			return
		}
	}
	if s.large.bmap == nil {
		s.toLarge()
	}
	bitMap, val := s.getOrMakeBitmap(i)
	bitMap.Set(val)
	key, val := keyVal(i)
	if key == 1 {
		if bitMap.hi == 805306368 {
			j := 0
			j++ // msirek-temp
		}
	}
}

// AddRange adds values 'from' up to 'to' (inclusively) to the set.
// E.g. AddRange(1,5) adds the values 1, 2, 3, 4, 5 to the set.
// 'to' must be >= 'from'.
// AddRange is always more efficient than individual Adds.
func (s *FastIntSet) AddRange(from, to int) {
	if to < from {
		panic("invalid range when adding range to FastIntSet")
	}

	if s.large == nil && from >= 0 && to < smallCutoff {
		s.small.SetRange(from, to)
	} else {
		for i := from; i <= to; i++ {
			s.Add(i)
		}
	}
}

func (s *FastIntSet2) AddRange(from, to int) {
	if to < from {
		panic(errors.AssertionFailedf("invalid range when adding range to FastIntSet"))
	}

	if s.large.bmap == nil && from >= 0 && to < smallCutoff {
		s.small.SetRange(from, to)
		return
	}
	for i := from; i <= to; i++ {
		s.Add(i)
	}
}

// Remove removes a value from the set. No-op if the value is not in the set.
func (s *FastIntSet) Remove(i int) {
	if i >= 0 && i < smallCutoff {
		s.small.Unset(i)
	}
	if s.large != nil {
		s.large.Remove(i)
	}
}

func (s *FastIntSet2) Remove(i int) {
	if isSmall(i) {
		s.small.Unset(i)
	}
	if s.large.bmap != nil {
		s.large.remove(i)
	}
}

// Contains returns true if the set contains the value.
func (s FastIntSet) Contains(i int) bool {
	if i >= 0 && i < smallCutoff {
		return s.small.IsSet(i)
	}
	if s.large != nil {
		return s.large.Has(i)
	}
	return false
}

func (s *FastIntSet2) Contains(i int) bool {
	if i >= 0 && i < smallCutoff {
		return s.small.IsSet(i)
	}
	if s.large.bmap != nil {
		return s.large.contains(i)
	}
	return false
}

// Empty returns true if the set is empty.
func (s FastIntSet) Empty() bool {
	return s.small == bitmap{} && (s.large == nil || s.large.IsEmpty())
}

func (s *FastIntSet2) Empty() bool {
	return s.small.isEmpty() && s.large.isEmpty()
}

// Len returns the number of the elements in the set.
func (s FastIntSet) Len() int {
	if s.large == nil {
		return s.small.OnesCount()
	}
	return s.large.Len()
}

func (s *FastIntSet2) Len() int {
	if s.large.bmap == nil {
		return s.small.OnesCount()
	}
	var count int
	for _, element := range s.large.bmap {
		count += element.OnesCount()
	}
	return count
}

// Next returns the first value in the set which is >= startVal. If there is no
// value, the second return value is false.
func (s FastIntSet) Next(startVal int) (int, bool) {
	if startVal < 0 && s.large == nil {
		startVal = 0
	}
	if startVal >= 0 && startVal < smallCutoff {
		if nextVal, ok := s.small.Next(startVal); ok {
			return nextVal, true
		}
	}
	if s.large != nil {
		res := s.large.LowerBound(startVal)
		return res, res != intsets.MaxInt
	}
	return intsets.MaxInt, false
}

func (s *FastIntSet2) getIntFromNextBitmap(key int) (int, bool) {
	nextKeyIdx := sort.SearchInts(s.large.sortedKeys, key+1)
	if nextKeyIdx >= len(s.large.sortedKeys) {
		return intsets.MaxInt, false
	}
	nextKey := s.large.sortedKeys[nextKeyIdx]
	if bmap, ok := s.large.bmap[nextKey]; ok {
		nextVal := bmap.Min()
		return intFromKeyVal(nextKey, nextVal), true
	}
	panic(errors.AssertionFailedf("bitmap exists with no values"))
}

func (s *FastIntSet2) Next(startVal int) (int, bool) {
	if startVal < 0 && s.large.bmap == nil {
		startVal = 0
	}
	if startVal >= 0 && startVal < smallCutoff {
		if nextVal, ok := s.small.Next(startVal); ok {
			return nextVal, true
		}
	}
	key, val := keyVal(startVal)
	if bmap, ok := s.large.getBitmap(startVal); ok {
		nextVal, ok := bmap.Next(val)
		if !ok {
			return s.getIntFromNextBitmap(key)
		}
		return intFromKeyVal(key, nextVal), true
	}
	return s.getIntFromNextBitmap(key)
}

// ForEach calls a function for each value in the set (in increasing order).
func (s FastIntSet) ForEach(f func(i int)) {
	if !s.fitsInSmall() {
		for x := s.large.Min(); x != intsets.MaxInt; x = s.large.LowerBound(x + 1) {
			f(x)
		}
		return
	}
	for v := s.small.lo; v != 0; {
		i := bits.TrailingZeros64(v)
		f(i)
		v &^= 1 << uint(i)
	}
	for v := s.small.hi; v != 0; {
		i := bits.TrailingZeros64(v)
		f(64 + i)
		v &^= 1 << uint(i)
	}
}

// ForEach calls a function for each value in the set (in increasing order).
func (s *FastIntSet2) ForEach(f func(i int)) {
	if s.Empty() {
		return
	}
	if !s.fitsInSmall() {
		for x := s.large.Min(); x != intsets.MaxInt; x, _ = s.Next(x + 1) {
			f(x)
		}
		return
	}
	for v := s.small.lo; v != 0; {
		i := bits.TrailingZeros64(v)
		f(i)
		v &^= 1 << uint(i)
	}
	for v := s.small.hi; v != 0; {
		i := bits.TrailingZeros64(v)
		f(64 + i)
		v &^= 1 << uint(i)
	}
}

// Ordered returns a slice with all the integers in the set, in increasing order.
func (s FastIntSet) Ordered() []int {
	if s.Empty() {
		return nil
	}
	if s.large != nil {
		return s.large.AppendTo([]int(nil))
	}
	result := make([]int, 0, s.Len())
	s.ForEach(func(i int) {
		result = append(result, i)
	})
	return result
}

// Ordered returns a slice with all the integers in the set, in increasing order.
func (s *FastIntSet2) Ordered() []int {
	if s.Empty() {
		return nil
	}
	result := make([]int, 0, s.Len())
	s.ForEach(func(i int) {
		result = append(result, i)
	})
	return result
}

// Copy returns a copy of s which can be modified independently.
func (s FastIntSet) Copy() FastIntSet {
	var c FastIntSet
	c.small = s.small
	if s.large != nil {
		c.large = new(intsets.Sparse)
		c.large.Copy(s.large)
	}
	return c
}

// Copy returns a copy of s which can be modified independently.
func (s *FastIntSet2) Copy() FastIntSet2 {
	var c FastIntSet2
	c.CopyFrom(s)
	return c
}

// CopyFrom sets the receiver to a copy of other, which can then be modified
// independently.
func (s *FastIntSet) CopyFrom(other FastIntSet) {
	s.small = other.small
	if other.large != nil {
		if s.large == nil {
			s.large = new(intsets.Sparse)
		}
		s.large.Copy(other.large)
	} else {
		if s.large != nil {
			s.large.Clear()
		}
	}
}

// CopyFrom sets the receiver to a copy of other, which can then be modified
// independently.
func (s *FastIntSet2) CopyFrom(other *FastIntSet2) {
	s.small.copyFrom(&other.small)
	s.large.copyFrom(&other.large)
}

// UnionWith adds all the elements from rhs to this set.
func (s *FastIntSet) UnionWith(rhs FastIntSet) {
	s.small.UnionWith(rhs.small)
	if s.large == nil && rhs.large == nil {
		// Fast path.
		return
	}

	if s.large == nil {
		s.large = s.toLarge()
	}
	if rhs.large == nil {
		for i, ok := rhs.Next(0); ok; i, ok = rhs.Next(i + 1) {
			s.large.Insert(i)
		}
	} else {
		s.large.UnionWith(rhs.large)
	}
}

// UnionWith adds all the elements from rhs to this set.
func (s *FastIntSet2) UnionWith(rhs FastIntSet2) {
	s.small.UnionWith(rhs.small)
	if s.large.bmap == nil && rhs.large.bmap == nil {
		// Fast path.
		return
	}

	if s.large.bmap == nil {
		s.toLarge()
	}
	// Union all the bitmaps with matching keys.
	for key, bmap := range s.large.bmap {
		if rhsBmap, ok := rhs.large.bmap[key]; ok {
			bmap.UnionWith(*rhsBmap)
		}
	}
	keyAdded := false
	// Add all the keys in rhs which don't exist in this FastIntSet.
	for key, rhsBmap := range rhs.large.bmap {
		if _, ok := s.large.bmap[key]; !ok {
			keyAdded = true
			bmap := &bitmap{}
			bmap.copyFrom(rhsBmap)
			s.large.bmap[key] = bmap
			s.large.sortedKeys = append(s.large.sortedKeys, key)
		}
	}
	if keyAdded {
		sort.Sort(s.large.sortedKeys)
	}
}

// Union returns the union of s and rhs as a new set.
func (s FastIntSet) Union(rhs FastIntSet) FastIntSet {
	r := s.Copy()
	r.UnionWith(rhs)
	return r
}

// Union returns the union of s and rhs as a new set.
func (s *FastIntSet2) Union(rhs FastIntSet2) FastIntSet2 {
	r := s.Copy()
	r.UnionWith(rhs)
	return r
}

// IntersectionWith removes any elements not in rhs from this set.
func (s *FastIntSet) IntersectionWith(rhs FastIntSet) {
	s.small.IntersectionWith(rhs.small)
	if rhs.large == nil {
		s.large = nil
	}
	if s.large == nil {
		// Fast path.
		return
	}

	s.large.IntersectionWith(rhs.toLarge())
}

// IntersectionWith removes any elements not in rhs from this set.
func (s *FastIntSet2) IntersectionWith(rhs FastIntSet2) {
	s.small.IntersectionWith(rhs.small)
	if rhs.large.bmap == nil {
		s.large.bmap = nil
		s.large.sortedKeys = nil
	}
	if s.large.bmap == nil {
		// Fast path.
		return
	}
	for key, bmap := range s.large.bmap {
		if rhsBmap, ok := rhs.large.bmap[key]; ok {
			bmap.IntersectionWith(*rhsBmap)
			if bmap.isEmpty() {
				delete(s.large.bmap, key)
				s.large.delete(key)
			}
		} else {
			delete(s.large.bmap, key)
			s.large.delete(key)
		}
	}
}

// Intersection returns the intersection of s and rhs as a new set.
func (s FastIntSet) Intersection(rhs FastIntSet) FastIntSet {
	r := s.Copy()
	r.IntersectionWith(rhs)
	return r
}

// Intersection returns the intersection of s and rhs as a new set.
func (s *FastIntSet2) Intersection(rhs FastIntSet2) FastIntSet2 {
	r := s.Copy()
	r.IntersectionWith(rhs)
	return r
}

// Intersects returns true if s has any elements in common with rhs.
func (s FastIntSet) Intersects(rhs FastIntSet) bool {
	if s.small.Intersects(rhs.small) {
		return true
	}
	if s.large == nil || rhs.large == nil {
		return false
	}
	return s.large.Intersects(rhs.toLarge())
}

// Intersects returns true if s has any elements in common with rhs.
func (s *FastIntSet2) Intersects(rhs FastIntSet2) bool {
	if s.small.Intersects(rhs.small) {
		return true
	}
	if s.large.bmap == nil || rhs.large.bmap == nil {
		return false
	}
	for key, bmap := range s.large.bmap {
		if rhsBmap, ok := rhs.large.bmap[key]; ok {
			if bmap.Intersects(*rhsBmap) {
				return true
			}
		}
	}
	return false
}

// DifferenceWith removes any elements in rhs from this set.
func (s *FastIntSet) DifferenceWith(rhs FastIntSet) {
	s.small.DifferenceWith(rhs.small)
	if s.large == nil {
		// Fast path
		return
	}
	s.large.DifferenceWith(rhs.toLarge())
}

// DifferenceWith removes any elements in rhs from this set.
func (s *FastIntSet2) DifferenceWith(rhs FastIntSet2) {
	s.small.DifferenceWith(rhs.small)
	if s.large.bmap == nil {
		// Fast path
		return
	}
	for key, bmap := range s.large.bmap {
		if rhsBmap, ok := rhs.large.bmap[key]; ok {
			bmap.DifferenceWith(*rhsBmap)
			if bmap.isEmpty() {
				delete(s.large.bmap, key)
				s.large.delete(key)
			}
		}
	}
}

// Difference returns the elements of s that are not in rhs as a new set.
func (s FastIntSet) Difference(rhs FastIntSet) FastIntSet {
	r := s.Copy()
	r.DifferenceWith(rhs)
	return r
}

// Difference returns the elements of s that are not in rhs as a new set.
func (s *FastIntSet2) Difference(rhs FastIntSet2) FastIntSet2 {
	r := s.Copy()
	r.DifferenceWith(rhs)
	return r
}

// Equals returns true if the two sets are identical.
func (s FastIntSet) Equals(rhs FastIntSet) bool {
	if s.small != rhs.small {
		return false
	}
	if s.fitsInSmall() {
		// We already know that the `small` fields are equal. We just have to make
		// sure that the other set also has no large elements.
		return rhs.fitsInSmall()
	}
	// We know that s has large elements.
	return rhs.large != nil && s.large.Equals(rhs.large)
}

// Equals returns true if the two sets are identical.
func (s *FastIntSet2) Equals(rhs FastIntSet2) bool {
	if s.small != rhs.small {
		return false
	}
	if s.fitsInSmall() {
		// We already know that the `small` fields are equal. We just have to make
		// sure that the other set also has no large elements.
		return rhs.fitsInSmall()
	}
	// We know that s has large elements.
	return s.large.Equals(&rhs.large)
}

// SubsetOf returns true if rhs contains all the elements in s.
func (s FastIntSet) SubsetOf(rhs FastIntSet) bool {
	if s.fitsInSmall() {
		return s.small.SubsetOf(rhs.small)
	}
	if rhs.fitsInSmall() {
		// s doesn't fit in small and rhs does.
		return false
	}
	return s.large.SubsetOf(rhs.large)
}

// SubsetOf returns true if rhs contains all the elements in s.
func (s *FastIntSet2) SubsetOf(rhs FastIntSet2) bool {
	if s.fitsInSmall() {
		return s.small.SubsetOf(rhs.small)
	}
	if rhs.fitsInSmall() {
		// s doesn't fit in small and rhs does.
		return false
	}
	for key, bmap := range s.large.bmap {
		if rhsBmap, ok := rhs.large.bmap[key]; ok {
			if !bmap.SubsetOf(*rhsBmap) {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

// Encode the set and write it to a bytes.Buffer using binary.varint byte
// encoding.
//
// This method cannot be used if the set contains negative elements.
//
// If the set has only elements in the range [0, 63], we encode a 0 followed by
// a 64-bit bitmap. Otherwise, we encode a length followed by each element.
//
// WARNING: this is used by plan gists, so if this encoding changes,
// explain.gistVersion needs to be bumped.
func (s *FastIntSet) Encode(buf *bytes.Buffer) error {
	if s.large != nil && s.large.Min() < 0 {
		return errors.AssertionFailedf("Encode used with negative elements")
	}

	// This slice should stay on stack. We only need enough bytes to encode a 0
	// and then an arbitrary 64-bit integer.
	//gcassert:noescape
	tmp := make([]byte, binary.MaxVarintLen64+1)

	if s.small.hi == 0 && s.fitsInSmall() {
		n := binary.PutUvarint(tmp, 0)
		n += binary.PutUvarint(tmp[n:], s.small.lo)
		buf.Write(tmp[:n])
	} else {
		n := binary.PutUvarint(tmp, uint64(s.Len()))
		buf.Write(tmp[:n])
		for i, ok := s.Next(0); ok; i, ok = s.Next(i + 1) {
			n := binary.PutUvarint(tmp, uint64(i))
			buf.Write(tmp[:n])
		}
	}
	return nil
}

// Decode does the opposite of Encode. The contents of the receiver are
// overwritten.
func (s *FastIntSet) Decode(br io.ByteReader) error {
	length, err := binary.ReadUvarint(br)
	if err != nil {
		return err
	}
	*s = FastIntSet{}

	if length == 0 {
		// Special case: a 64-bit bitmap is encoded directly.
		val, err := binary.ReadUvarint(br)
		if err != nil {
			return err
		}
		s.small.lo = val
	} else {
		for i := 0; i < int(length); i++ {
			elem, err := binary.ReadUvarint(br)
			if err != nil {
				*s = FastIntSet{}
				return err
			}
			s.Add(int(elem))
		}
	}
	return nil
}

func (v bitmap) IsSet(i int) bool {
	w := v.lo
	if i >= 64 {
		w = v.hi
	}
	return w&(1<<uint64(i&63)) != 0
}

func (v *bitmap) Set(i int) {
	if i < 64 {
		v.lo |= (1 << uint64(i))
	} else {
		v.hi |= (1 << uint64(i&63))
	}
}

func (v *bitmap) Unset(i int) {
	if i < 64 {
		v.lo &= ^(1 << uint64(i))
	} else {
		v.hi &= ^(1 << uint64(i&63))
	}
}

func (v *bitmap) SetRange(from, to int) {
	mask := func(from, to int) uint64 {
		return (1<<(to-from+1) - 1) << from
	}
	switch {
	case to < 64:
		v.lo |= mask(from, to)
	case from >= 64:
		v.hi |= mask(from&63, to&63)
	default:
		v.lo |= mask(from, 63)
		v.hi |= mask(0, to&63)
	}
}

func (v *bitmap) UnionWith(other bitmap) {
	v.lo |= other.lo
	v.hi |= other.hi
}

func (v *bitmap) IntersectionWith(other bitmap) {
	v.lo &= other.lo
	v.hi &= other.hi
}

func (v bitmap) Intersects(other bitmap) bool {
	return ((v.lo & other.lo) | (v.hi & other.hi)) != 0
}

func (v *bitmap) DifferenceWith(other bitmap) {
	v.lo &^= other.lo
	v.hi &^= other.hi
}

func (v bitmap) SubsetOf(other bitmap) bool {
	return (v.lo&other.lo == v.lo) && (v.hi&other.hi == v.hi)
}

func (v bitmap) OnesCount() int {
	return bits.OnesCount64(v.lo) + bits.OnesCount64(v.hi)
}

func (v bitmap) Next(startVal int) (nextVal int, ok bool) {
	if startVal < 64 {
		if ntz := bits.TrailingZeros64(v.lo >> uint64(startVal)); ntz < 64 {
			// Found next element in the low word.
			return startVal + ntz, true
		}
		startVal = 64
	}
	// Check high word.
	if ntz := bits.TrailingZeros64(v.hi >> uint64(startVal&63)); ntz < 64 {
		return startVal + ntz, true
	}
	return -1, false
}

func (v bitmap) Max() int {
	leadingHi := bits.LeadingZeros64(v.hi)
	if leadingHi != 64 {
		return 127 - leadingHi
	}
	leadingLo := bits.LeadingZeros64(v.lo)
	if leadingLo == 64 {
		panic(errors.AssertionFailedf("no max value for bitmap"))
	}
	return 63 - leadingLo
}

func (v bitmap) Min() int {
	trailingLo := bits.TrailingZeros64(v.lo)
	if trailingLo != 64 {
		return trailingLo
	}
	trailingHi := bits.TrailingZeros64(v.hi)
	if trailingHi == 64 {
		panic(errors.AssertionFailedf("no min value for bitmap"))
	}
	return trailingHi + 64
}
