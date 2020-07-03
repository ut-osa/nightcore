package ipc

import (
	"fmt"
	"os"
	"syscall"
)

type ShmRegion struct {
	Name string
	Data []byte
	Size int
}

func ShmCreate(name string, size int) (*ShmRegion, error) {
	flags := syscall.O_CREAT | syscall.O_EXCL | syscall.O_RDWR
	fd, err := syscall.Open(shmFullPath(name), flags, fileCreatMode)
	if err != nil {
		return nil, fmt.Errorf("open failed: %v", err)
	}
	defer syscall.Close(fd)
	err = syscall.Ftruncate(fd, int64(size))
	if err != nil {
		return nil, fmt.Errorf("ftruncate failed: %v", err)
	}
	if size > 0 {
		flags = syscall.PROT_READ | syscall.PROT_WRITE
		data, err := syscall.Mmap(fd, 0, size, flags, syscall.MAP_SHARED)
		if err != nil {
			return nil, fmt.Errorf("mmap failed: %v", err)
		}
		return &ShmRegion{
			Name: name,
			Data: data,
			Size: size,
		}, nil
	} else {
		return &ShmRegion{
			Name: name,
			Data: nil,
			Size: 0,
		}, nil
	}
}

func ShmOpen(name string, readonly bool) (*ShmRegion, error) {
	flags := syscall.O_RDWR
	if readonly {
		flags = syscall.O_RDONLY
	}
	fd, err := syscall.Open(shmFullPath(name), flags, 0)
	if err != nil {
		return nil, fmt.Errorf("open failed: %v", err)
	}
	defer syscall.Close(fd)
	var stat syscall.Stat_t
	err = syscall.Fstat(fd, &stat)
	if err != nil {
		return nil, fmt.Errorf("fstat failed: %v", err)
	}
	if stat.Size > 0 {
		size := int(stat.Size)
		flags = syscall.PROT_READ
		if !readonly {
			flags |= syscall.PROT_WRITE
		}
		data, err := syscall.Mmap(fd, 0, size, flags, syscall.MAP_SHARED)
		if err != nil {
			return nil, fmt.Errorf("mmap failed: %v", err)
		}
		return &ShmRegion{
			Name: name,
			Data: data,
			Size: size,
		}, nil
	} else {
		return &ShmRegion{
			Name: name,
			Data: nil,
			Size: 0,
		}, nil
	}
}

func (r *ShmRegion) Close() {
	if r.Size > 0 {
		syscall.Munmap(r.Data)
		r.Data = nil
	}
}

func (r *ShmRegion) Remove() {
	os.Remove(shmFullPath(r.Name))
}

func shmFullPath(shmName string) string {
	return fmt.Sprintf("%s/shm/%s", rootPathForIpc, shmName)
}
