package files

import "os"

type osFileSystem struct{}

func (fs *osFileSystem) OpenFile(path string) (File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	return &osFile{
		path:      path,
		handle:    f,
		size:      info.Size(),
		isOpen:    true,
		fsHandler: fs,
	}, nil
}

func (fs *osFileSystem) CreateFile(path string) (File, error) {
	if f, err := os.Create(path); err == nil {
		return &osFile{
			path:      path,
			handle:    f,
			size:      0,
			isOpen:    true,
			fsHandler: fs,
		}, nil
	} else {
		return nil, err
	}
}

func (fs *osFileSystem) DeleteFile(file File) error {
	return os.Remove(file.Path())
}

func (fs *osFileSystem) FileDoesNotExistError() error {
	return os.ErrNotExist
}
