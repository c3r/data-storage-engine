package files

import (
	"os"
)

type osFile struct {
	handle    *os.File
	path      string
	size      int64
	isOpen    bool
	fsHandler FileSystem
}

func (file *osFile) refreshHandle() error {
	if !file.isOpen {
		f, err := file.fsHandler.OpenFile(file.path)
		if err != nil {
			return err
		}
		_file := f.(*osFile)
		file.handle = _file.handle
		file.isOpen = true
		return nil
	}
	return nil
}

func (file *osFile) Path() string { return file.path }
func (file *osFile) Size() int64  { return file.size }
func (file *osFile) Sync() error  { return file.handle.Sync() }

func (file *osFile) Close() error {
	if file.isOpen {
		file.isOpen = false
		return file.handle.Close()
	}
	return nil
}

func (file *osFile) Append(b []byte) error {
	if err := file.refreshHandle(); err != nil {
		return err
	}
	bytesWritten, err := file.handle.WriteAt(b, file.size)
	file.size += int64(bytesWritten)
	return err
}

func (file *osFile) ReadAt(b *[]byte, offset int64) error {
	if err := file.refreshHandle(); err != nil {
		return err
	}
	_, err := file.handle.ReadAt(*b, offset)
	return err
}
