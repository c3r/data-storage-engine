package files

import (
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/google/uuid"
)

func OpenFileWrite(filePath string) (*os.File, error) {
	var file *os.File
	if _, err := os.Stat(filePath); err == nil {
		file, err = os.OpenFile(filePath, os.O_WRONLY, 0666)
		if err != nil {
			return nil, err
		}
	} else if errors.Is(err, os.ErrNotExist) {
		file, err = os.Create(filePath)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}
	return file, nil
}

func Delete(filePath string) error {
	if err := os.Remove(filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func OpenFileRead(filePath string) (*os.File, fs.FileInfo, error) {
	var file *os.File
	if info, err := os.Stat(filePath); err == nil {
		file, err = os.Open(filePath)
		if err != nil {
			return nil, nil, err
		}
		return file, info, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return nil, nil, err
	} else {
		return nil, nil, err
	}
}

func Read(file *os.File, size uint64, offset uint64) ([]byte, error) {
	bytesBuffer := make([]byte, size)
	_, err := file.ReadAt(bytesBuffer, int64(offset))
	if err != nil {
		return nil, err
	}
	return bytesBuffer, nil
}

func Close(file *os.File) error {
	err := file.Sync()
	if err != nil {
		return err
	}
	err = file.Close()
	if err != nil {
		return err
	}
	return nil
}

func Create(dir string) (string, error) {
	filePath := fmt.Sprintf(dir + "/" + uuid.New().String()) // TODO: handle slash ending
	file, err := OpenFileWrite(filePath)
	if err != nil {
		return "", err
	}
	err = Close(file)
	if err != nil {
		return "", err
	}
	return filePath, nil
}
