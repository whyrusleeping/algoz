package main

import (
	"time"
)

const alpha = "234567abcdefghijklmnopqrstuvwxyz"

func s32encode(i uint64) string {
	var s string
	for i > 0 {
		c := i & 0x1f
		i = i >> 5
		s = alpha[c:c+1] + s
	}
	return s
}

func TID(ts time.Time) string {
	t := uint64(ts.UnixMicro())

	return s32encode(uint64(t)) + s32encode(0)
}
