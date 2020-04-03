package main

import (
	"fmt"
	"os"
	"syscall"
)

type SharedMemory struct {
	basePath string
}

func newSharedMemory(basePath string) (*SharedMemory, error) {
	_, err := os.Stat(basePath)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("Shared memory path \"%s\" does not exist", basePath)
	}
	return &SharedMemory{
		basePath: basePath,
	}, nil
}

func (s *SharedMemory) create(path string, size int) ([]byte, error) {
	fd, err := syscall.Open(
		s.basePath+"/"+path, syscall.O_CREAT|syscall.O_EXCL|syscall.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open failed: %v", err)
	}
	defer syscall.Close(fd)
	err = syscall.Ftruncate(fd, int64(size))
	if err != nil {
		return nil, fmt.Errorf("ftruncate failed: %v", err)
	}
	if size > 0 {
		data, err := syscall.Mmap(
			fd, 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			return nil, fmt.Errorf("mmap failed: %v", err)
		}
		return data, nil
	} else {
		return []byte{}, nil
	}
}

func (s *SharedMemory) openReadOnly(path string) ([]byte, error) {
	fd, err := syscall.Open(s.basePath+"/"+path, syscall.O_RDONLY, 0)
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
		data, err := syscall.Mmap(
			fd, 0, int(stat.Size), syscall.PROT_READ, syscall.MAP_SHARED)
		if err != nil {
			return nil, fmt.Errorf("mmap failed: %v", err)
		}
		return data, nil
	} else {
		return []byte{}, nil
	}
}

func (s *SharedMemory) close(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	err := syscall.Munmap(data)
	if err != nil {
		return fmt.Errorf("munmap failed: %v", err)
	} else {
		return nil
	}
}

func (s *SharedMemory) remove(path string) error {
	return os.Remove(s.basePath + "/" + path)
}
