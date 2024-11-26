package files

import (
	"errors"
	"os"
)

type FileSystem interface {
	OpenFile(string) (File, error)
	CreateFile(string) (File, error)
	DeleteFile(File) error
	FileDoesNotExistError() error
}

type File interface {
	Path() string
	Size() int64
	Append([]byte) error
	Sync() error
	Close() error
	ReadAt(*[]byte, int64) error
}

var (
	osfs osFileSystem
	mfs  memFileSystem
)

var fs = osfs

func OpenWrite(filePath string) (File, error) {
	file, err := fs.OpenFile(filePath)
	if err != nil {
		if errors.Is(err, fs.FileDoesNotExistError()) {
			file, err = fs.CreateFile(filePath)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	return file, nil
}

func OpenRead(filePath string) (File, error) {
	if file, err := fs.OpenFile(filePath); err == nil {
		return file, nil
	} else {
		return nil, err
	}
}

func Close(file File) error {
	if err := file.Sync(); err != nil {
		return err
	}
	return file.Close()
}

func Delete(file File) error {
	if err := fs.DeleteFile(file); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
