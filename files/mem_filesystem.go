package files

import (
	"os"
)

type memFileSystem struct {
	files map[string]*memFile
}

type memFile struct {
	data   []byte
	path   string
	length int64
}

func (file *memFile) Path() string { return file.path }
func (file *memFile) Size() int64  { return int64(len(file.data)) }
func (file *memFile) Sync() error  { return nil }

func (file *memFile) Close() error {
	return nil
}

func (file *memFile) Append(b []byte) error {
	file.data = append(file.data, b...)
	return nil
}

func (file *memFile) ReadAt(b *[]byte, offset int64) error {
	hb := offset + int64(len(*b))
	*b = file.data[offset:hb]
	return nil
}

func (fs *memFileSystem) OpenFile(path string) (File, error) {
	if fs.files[path] == nil {
		return nil, fs.FileDoesNotExistError()
	}
	return fs.files[path], nil
}

func (fs *memFileSystem) CreateFile(path string) (File, error) {
	if fs.files == nil {
		fs.files = map[string]*memFile{}
	}
	f := &memFile{
		path:   path,
		length: 0,
	}
	fs.files[path] = f
	return f, nil
}

func (fs *memFileSystem) DeleteFile(file File) error {
	fs.files[file.Path()] = nil
	return nil
}

func (fs *memFileSystem) FileDoesNotExistError() error {
	return os.ErrNotExist
}
