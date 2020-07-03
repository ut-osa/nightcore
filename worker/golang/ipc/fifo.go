package ipc

import (
	"fmt"
	"os"
	"syscall"
)

func FifoCreate(name string) error {
	return syscall.Mkfifo(fmt.Sprintf("%s/fifo/%s", rootPathForIpc, name), fileCreatMode)
}

func FifoRemove(name string) {
	os.Remove(fmt.Sprintf("%s/fifo/%s", rootPathForIpc, name))
}

func FifoOpenForRead(name string, nonblocking bool) (*os.File, error) {
	fifoPath := fmt.Sprintf("%s/fifo/%s", rootPathForIpc, name)
	flags := syscall.O_RDONLY
	if nonblocking {
		flags |= syscall.O_NONBLOCK
	}
	fd, err := syscall.Open(fifoPath, flags, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), fifoPath), nil
}

func FifoOpenForWrite(name string, nonblocking bool) (*os.File, error) {
	fifoPath := fmt.Sprintf("%s/fifo/%s", rootPathForIpc, name)
	flags := syscall.O_WRONLY
	if nonblocking {
		flags |= syscall.O_NONBLOCK
	}
	fd, err := syscall.Open(fifoPath, flags, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), fifoPath), nil
}

func FifoOpenForReadWrite(name string, nonblocking bool) (*os.File, error) {
	fifoPath := fmt.Sprintf("%s/fifo/%s", rootPathForIpc, name)
	flags := syscall.O_RDWR
	if nonblocking {
		flags |= syscall.O_NONBLOCK
	}
	fd, err := syscall.Open(fifoPath, flags, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), fifoPath), nil
}
