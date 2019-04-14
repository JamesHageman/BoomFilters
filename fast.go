package boom

import (
	"hash"
	"hash/fnv"
	"sync"
	"sync/atomic"
)

type LockFreeBloomFilter struct {
	data     []uint32
	m        uint // filter size
	k        uint // number of hash functions
	count    uint // number of items added
	hashPool sync.Pool
}

var _ Filter = &LockFreeBloomFilter{}

func NewLockFreeBloomFilter(n uint, fpRate float64) *LockFreeBloomFilter {
	m := OptimalM(n, fpRate)
	return &LockFreeBloomFilter{
		data:  make([]uint32, m/32+1),
		m:     m,
		k:     OptimalK(fpRate),
		count: 0,
		hashPool: sync.Pool{
			New: func() interface{} {
				return fnv.New64()
			},
		},
	}
}

// Capacity returns the Bloom filter capacity, m.
func (f *LockFreeBloomFilter) Capacity() uint {
	return f.m
}

// K returns the number of hash functions.
func (f *LockFreeBloomFilter) K() uint {
	return f.k
}

func (f *LockFreeBloomFilter) Test(key []byte) bool {
	lower, upper := f.hashKernelLockFree(key)

	// If any of the K bits are not set, then it's not a member.
	for i := uint(0); i < f.k; i++ {
		offset := (uint(lower) + uint(upper)*i) % f.m

		if !f.getBit(offset) {
			return false
		}
	}

	return true
}

func (f *LockFreeBloomFilter) Add(key []byte) Filter {
	lower, upper := f.hashKernelLockFree(key)

	// Set all k bits to 1
	for i := uint(0); i < f.k; i++ {
		offset := (uint(lower) + uint(upper)*i) % f.m
		f.setBit(offset)
	}

	return f
}

func (f *LockFreeBloomFilter) TestAndAdd(key []byte) bool {
	lower, upper := f.hashKernelLockFree(key)
	member := true

	// If any of the K bits are not set, then it's not a member.
	for i := uint(0); i < f.k; i++ {
		offset := (uint(lower) + uint(upper)*i) % f.m

		if !f.getBit(offset) {
			member = false
		}

		f.setBit(offset)
	}

	return member
}

func (f *LockFreeBloomFilter) hashKernelLockFree(data []byte) (uint32, uint32) {
	h := f.hashPool.Get().(hash.Hash64)
	h.Write(data)
	sum := h.Sum64()
	higher := uint32(sum >> 32)
	lower := uint32(sum)
	h.Reset()
	f.hashPool.Put(h)
	return higher, lower
}

func (f *LockFreeBloomFilter) getBit(offset uint) bool {
	byteIndex := offset / 32
	byteOffset := offset % 32
	mask := uint32(1 << byteOffset)

	b := f.data[byteIndex]
	return b&mask != 0
}

func (f *LockFreeBloomFilter) setBit(offset uint) {
	index := offset / 32
	bit := offset % 32
	mask := uint32(1 << bit)

	ptr := &f.data[index]

	for {
		orig := atomic.LoadUint32(ptr)
		updated := orig | mask
		swapped := atomic.CompareAndSwapUint32(ptr, orig, updated)
		if swapped {
			break
		}
	}
}
