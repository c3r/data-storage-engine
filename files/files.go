package files

import (
	"errors"
	"os"
)

var fileSystem = osFiles()

func OpenWrite(filePath string) (*File, error) {
	file, err := fileSystem.openFile(filePath)
	if err != nil {
		if errors.Is(err, fileSystem.FileDoesNotExistError) {
			file, err = fileSystem.createFile(filePath)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	return file, nil
}

func OpenRead(filePath string) (*File, error) {
	if file, err := fileSystem.openFile(filePath); err == nil {
		return file, nil
	} else {
		return nil, err
	}
}

func Close(file *File) error {
	if err := file.Sync(); err != nil {
		return err
	}
	return file.Close()
}

func Delete(file *File) error {
	if err := fileSystem.deleteFile(file.Path()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
