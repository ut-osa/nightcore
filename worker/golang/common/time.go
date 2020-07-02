package common

import (
	_ "unsafe" // required to use //go:linkname
)

//go:noescape
//go:linkname nanotime runtime.nanotime
func nanotime() int64

func GetMonotonicMicroTimestamp() int64 {
	return nanotime() / 1000
}
