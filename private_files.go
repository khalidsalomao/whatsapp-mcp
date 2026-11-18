package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

func readPrivateFile(path string, maxBytes int64) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("%s must be a private regular file (mode 0600 or 0400)", path)
	}
	data, err := io.ReadAll(io.LimitReader(f, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("%s is too large", path)
	}
	return data, nil
}

// Publish only complete files. A hard link creates a new file without
// replacing credentials created concurrently by another command.
func writePrivateFile(path string, data []byte, replace bool) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	_, writeErr := tmp.Write(data)
	closeErr := tmp.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	if replace {
		return os.Rename(tmp.Name(), path)
	}
	return os.Link(tmp.Name(), path)
}
