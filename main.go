package main

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"
)

const FilePathTemplate = "/tmp/tb_storage_%d"
const MemtableMaxSize = 2

type SegmentIndexItem struct {
	Offset uint64
	Size   uint64
}

type SegmentIndex struct {
	Id    int
	Index map[string]*SegmentIndexItem
}

type PersistentRow struct {
	Key   string
	Value string
}

var memtableOrder []string
var memtable = map[string]string{}
var indexes []*SegmentIndex

func main() {
	//babbler := babble.NewBabbler()
	//txt1 := babbler.Babble()

	Save("1", "a")
	Save("2", "b")
	Save("3", "c")
	Save("4", "d")
	Save("5", "e")
	Save("6", "f")
	Save("7", "g")
	Save("8", "h")
	Save("9", "i")
	Save("10", "j")

	Load("1")
	Load("2")
	Load("3")
	Load("4")
	Load("5")
	Load("6")
	Load("7")
	Load("8")
	Load("9")
	Load("10")

}

func Save(key string, value string) error {
	if len(memtable) == MemtableMaxSize {
		segmentIndex := newSegmentIndex()
		err := saveMemtableAsSegment(segmentIndex)
		if err != nil {
			return err
		}
		memtable = make(map[string]string)
		memtableOrder = nil
	}
	memtable[key] = value
	memtableOrder = append(memtableOrder, key)
	slices.Sort(memtableOrder)
	return nil
}

func Load(key string) (string, error) {
	value, valueExists := memtable[key]
	if valueExists {
		return value, nil
	}
	for segment := len(indexes) - 1; segment >= 0; segment-- {
		segmentIndex := indexes[segment]
		indexItem, valueExists := segmentIndex.Index[key]
		if !valueExists {
			continue
		}
		bytesFromFile, err := readSegment(indexItem.Offset, indexItem.Size, segment)
		if err != nil {
			return "", err
		}
		row, err := decode(bytesFromFile)
		if err != nil {
			return "", err
		}
		log(fmt.Sprintf("%s = %s", key, row.Value))
		return row.Value, nil
	}
	log(fmt.Sprintf("Key [%s] not found!", key))
	return "", errors.New("key not found")
}

func saveMemtableAsSegment(segmentIndex SegmentIndex) error {
	var writeOffset uint64 = 0
	for _, key := range memtableOrder {
		value := memtable[key]
		row := new(PersistentRow)
		row.Key = key
		row.Value = value
		encodedBytes, err := encode(row)
		if err != nil {
			log(fmt.Sprintf("Error while encoding row with key = [%s]", row.Key))
			return err
		}
		bytesWritten, err := writeSegment(encodedBytes, segmentIndex.Id, writeOffset)
		if err != nil {
			return err
		}
		segmentIndex.update(writeOffset, bytesWritten, key)
		writeOffset += bytesWritten
	}
	return nil
}

func newSegmentIndex() SegmentIndex {
	var index SegmentIndex
	indexes = append(indexes, &index)
	index.Index = make(map[string]*SegmentIndexItem)
	index.Id = len(indexes) - 1
	return index
}

func (index *SegmentIndex) update(writeOffset uint64, bytesWritten uint64, key string) {
	var item SegmentIndexItem
	item.Offset = writeOffset
	item.Size = bytesWritten
	index.Index[key] = &item
}

func getSegmentFilePath(segment int) string {
	return fmt.Sprintf(FilePathTemplate, segment)
}

//func compactSegmentFile(segmentNum int) error {
//	path := getSegmentFilePath(segmentNum)
//	file, err := openFileToRead(path)
//	if err != nil {
//		return err
//	}
//
//}

// ---------------------------------------------------------------------------------------------------------------------
// DATA OPERATIONS
// ---------------------------------------------------------------------------------------------------------------------

func writeSegment(encodedBytes []byte, segment int, writeOffset uint64) (uint64, error) {
	path := getSegmentFilePath(segment)
	file, err := openWrite(path)
	if err != nil {
		log(fmt.Sprintf("Error while opening file [%s] to write", path))
		return 0, err
	}
	bytesWritten, err := write(file, encodedBytes, writeOffset)
	if err != nil {
		log(fmt.Sprintf("Error while writing to file [%s]", path))
		return 0, err
	}
	return uint64(bytesWritten), nil
}

func readSegment(offset uint64, size uint64, segment int) ([]byte, error) {
	path := getSegmentFilePath(segment)
	file, err := openRead(path)
	if err != nil {
		log(fmt.Sprintf("Error while opening file [%s] to open", path))
		return nil, err
	}
	bytesFromFile, err := read(file, size, offset)
	if err != nil {
		log(fmt.Sprintf("Error while reading file [%s]", path))
		return nil, err
	}
	return bytesFromFile, nil
}

// ---------------------------------------------------------------------------------------------------------------------
// DATA DECODING/ENCODING
// ---------------------------------------------------------------------------------------------------------------------

func encode(row *PersistentRow) ([]byte, error) {
	bytesBuffer := bytes.Buffer{}
	gobEncoder := gob.NewEncoder(&bytesBuffer)
	err := gobEncoder.Encode(row)
	if err != nil {
		return nil, err
	}
	encodedRow := base64.StdEncoding.EncodeToString(bytesBuffer.Bytes())
	encodedBytes := []byte(encodedRow)
	return encodedBytes, nil
}

func decode(bytesFromFile []byte) (*PersistentRow, error) {
	var bytesBuffer bytes.Buffer
	var row PersistentRow
	stringBytes, err := base64.StdEncoding.DecodeString(string(bytesFromFile))
	if err != nil {
		return nil, err
	}
	bytesBuffer.Write(stringBytes)
	decoder := gob.NewDecoder(&bytesBuffer)
	err = decoder.Decode(&row)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// ---------------------------------------------------------------------------------------------------------------------
// FILES
// ---------------------------------------------------------------------------------------------------------------------
func openWrite(filePath string) (*os.File, error) {
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

func openRead(filePath string) (*os.File, error) {
	var file *os.File
	if _, err := os.Stat(filePath); err == nil {
		file, err = os.Open(filePath)
		if err != nil {
			return nil, err
		}
	} else if errors.Is(err, os.ErrNotExist) {
		return nil, err
	} else {
		return nil, err
	}
	return file, nil
}

func read(file *os.File, size uint64, offset uint64) ([]byte, error) {
	bytesBuffer := make([]byte, size)
	_, err := file.ReadAt(bytesBuffer, int64(offset))
	if err != nil {
		return nil, err
	}
	return bytesBuffer, nil
}

func write(file *os.File, bytes []byte, writeOffset uint64) (int, error) {
	bytesWritten, err := file.WriteAt(bytes, int64(writeOffset))
	if err != nil {
		return 0, err
	}
	err = file.Sync()
	if err != nil {
		return 0, err
	}
	err = file.Close()
	if err != nil {
		return 0, err
	}
	return bytesWritten, err
}

// ---------------------------------------------------------------------------------------------------------------------
// DEBUG Functions
// ---------------------------------------------------------------------------------------------------------------------

func log(format string) {
	now := time.Now().Format(time.StampNano)
	format = now + ": " + format + "\n"
	fmt.Printf(format)
}
