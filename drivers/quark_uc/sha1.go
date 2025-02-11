package quark

import (
	"crypto/sha1"
	"hash"
	"reflect"
	"unsafe"
)

type SHA1 struct {
	hash hash.Hash
	h    *[5]uint32
	nx   *int
	len  *uint64
}

func NewSHA1() *SHA1 {
	hash := sha1.New()
	rv := reflect.ValueOf(hash).Elem()
	hFiled := rv.FieldByName("h")
	h := (*[5]uint32)(unsafe.Pointer(hFiled.UnsafeAddr()))

	nxFiled := rv.FieldByName("nx")
	nx := (*int)(unsafe.Pointer(nxFiled.UnsafeAddr()))

	lenFiled := rv.FieldByName("len")
	len := (*uint64)(unsafe.Pointer(lenFiled.UnsafeAddr()))
	return &SHA1{hash, h, nx, len}
}

func (s *SHA1) Write(p []byte) (n int, err error) {
	return s.hash.Write(p)
}
func (s *SHA1) GetState() ([5]uint32, uint64) {
	return *s.h, *s.len
}
func (s *SHA1) Reset() {
	s.hash.Reset()
}

func offset2nlnh(offset uint64) (nl uint32, nh uint32) {
	nl = (uint32(offset) << 3) & 0xFFFFFFFF
	nh = (uint32(offset)) >> 29 & 0xFFFFFFFF
	return
}
