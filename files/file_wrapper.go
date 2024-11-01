package files

import (
	"os"
)

type fsHandler struct {
	openFile              func(path string) (*File, error)
	createFile            func(path string) (*File, error)
	deleteFile            func(path string) error
	FileDoesNotExistError error
}

type File struct {
	handle *os.File
	path   string
	size   int64
	Path   func() string
	Size   func() int64
	Append func(b []byte) error
	Sync   func() error
	Close  func() error
	ReadAt func(b []byte, offset int64) error
}

func osFiles() fsHandler {
	new_file := func(path string, handle *os.File, size int64) *File {
		file := &File{
			path:   path,
			handle: handle,
			size:   size,
		}
		file.Path = func() string { return file.path }
		file.Size = func() int64 { return file.size }
		file.Sync = func() error { return file.handle.Sync() }
		file.Close = func() error { return file.handle.Close() }
		file.Append = func(b []byte) error {
			bytesWritten, err := file.handle.WriteAt(b, file.size)
			file.size += int64(bytesWritten)
			return err
		}
		file.ReadAt = func(b []byte, offset int64) error {
			_, err := file.handle.ReadAt(b, offset)
			return err
		}
		return file
	}
	var file *os.File
	var fileInfo os.FileInfo
	var err error
	return fsHandler{
		openFile: func(path string) (*File, error) {
			if file, err = os.Open(path); err == nil {
				if fileInfo, err = os.Stat(path); err == nil {
					return new_file(path, file, fileInfo.Size()), nil
				}
			}
			return nil, err
		},
		createFile: func(path string) (*File, error) {
			if file, err = os.Create(path); err == nil {
				return new_file(path, file, 0), nil
			}
			return nil, err
		},
		deleteFile: func(path string) error {
			return os.Remove(path)
		},
		FileDoesNotExistError: os.ErrNotExist,
	}
}
