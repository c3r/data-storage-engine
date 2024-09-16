package main

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
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
var segmentIndexes []*SegmentIndex

func main() {
	//babbler := babble.NewBabbler()
	//txt1 := babbler.Babble()

	SaveKeyValueRow("1", "a")
	SaveKeyValueRow("2", "b")
	SaveKeyValueRow("3", "c")
	SaveKeyValueRow("4", "d")
	SaveKeyValueRow("5", "e")
	SaveKeyValueRow("6", "f")
	SaveKeyValueRow("7", "g")
	SaveKeyValueRow("8", "h")
	SaveKeyValueRow("9", "i")
	SaveKeyValueRow("10", "j")

	RetrieveValueForKey("1")
	RetrieveValueForKey("2")
	RetrieveValueForKey("3")
	RetrieveValueForKey("4")
	RetrieveValueForKey("5")
	RetrieveValueForKey("6")
	RetrieveValueForKey("7")
	RetrieveValueForKey("8")
	RetrieveValueForKey("9")
	RetrieveValueForKey("10")

}

func SaveKeyValueRow(key string, value string) error {
	// If the memtable is still small enough, keep saving into memory
	// Otherwise, save the memtable to disk while keeping saving to memory
	if len(memtable) == MemtableMaxSize {
		segmentNum := len(segmentIndexes)
		Debug("Saving memtable to disk as segment %s...", strconv.Itoa(segmentNum))
		Debug("Memtable to be saved: %s", fmt.Sprint(memtable))
		var writeOffset int64 = 0
		var segmentIndex SegmentIndex
		segmentIndex.Index = make(map[string]*SegmentIndexItem)
		for _, key := range memtableOrder { // In order!!!
			value = memtable[key]

			row := new(PersistentRow)
			row.Key = key
			row.Value = value

			bytesWritten, err := persistRow(row, segmentNum, writeOffset)
			if err != nil {
				return err
			}

			var indexItem SegmentIndexItem
			indexItem.Offset = uint64(writeOffset)
			indexItem.Size = bytesWritten
			segmentIndex.Index[key] = &indexItem
			writeOffset += int64(bytesWritten)
		}

		segmentIndexes = append(segmentIndexes, &segmentIndex)

		for idx := range segmentIndexes {
			Debug("Segment indexes: %s", fmt.Sprint(segmentIndexes[idx]))
		}

		// Reset memtable
		memtable = make(map[string]string)
		memtableOrder = nil
	}

	memtable[key] = value
	memtableOrder = append(memtableOrder, key)
	slices.Sort(memtableOrder)

	return nil
}

func RetrieveValueForKey(key string) (string, error) {
	value, valueExists := memtable[key]
	if valueExists {
		return value, nil
	}

	// If value does not exist in memtable, search segmentIndexes
	// When key is found in a segmentIndex, read the value from the segment file from disk
	Debug("Value for key %s not found in memtable, searching in segments...", key)
	for segmentNum, segmentIndex := range segmentIndexes {
		//Debug("Searching in segment %s...", strconv.Itoa(segmentNum))
		indexItem, valueExists := segmentIndex.Index[key]
		if !valueExists {
			continue
		}
		//Debug("Found in segment %s. Trying to retrieve from segment file...", strconv.Itoa(segmentNum))
		row, err := retrieveRow(indexItem.Offset, indexItem.Size, segmentNum)
		if err != nil {
			return "", err
		}
		Debug("Value %s retrieved!", row.Value)
		return row.Value, nil
	}

	return "", errors.New("key not found")
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

func persistRow(row *PersistentRow, segmentNum int, writeOffset int64) (uint64, error) {
	pathForSegmentFile := getSegmentFilePath(segmentNum)
	file, err := openFileToWrite(pathForSegmentFile)
	if err != nil {
		return 0, err
	}

	encodedBytes, err := encodeKeyValueRow(row)
	if err != nil {
		return 0, err
	}

	bytesWritten, err := writeToFile(file, encodedBytes, writeOffset)
	if err != nil {
		return 0, err
	}

	return uint64(bytesWritten), nil
}

func retrieveRow(offset uint64, size uint64, segmentNum int) (*PersistentRow, error) {
	pathForSegmentFile := getSegmentFilePath(segmentNum)
	file, err := openFileToRead(pathForSegmentFile)
	if err != nil {
		return nil, err
	}

	bytesFromFile, err := readFromFile(file, size, offset)
	if err != nil {
		return nil, err
	}

	keyValueRow, err := decodeKeyValueRow(bytesFromFile)
	if err != nil {
		return nil, err
	}

	return keyValueRow, nil
}

// ---------------------------------------------------------------------------------------------------------------------
// DATA DECODING/ENCODING
// ---------------------------------------------------------------------------------------------------------------------

func encodeKeyValueRow(row *PersistentRow) ([]byte, error) {
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

func decodeKeyValueRow(bytesFromFile []byte) (*PersistentRow, error) {
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
func openFileToWrite(filePath string) (*os.File, error) {
	var file *os.File
	if _, err := os.Stat(filePath); err == nil {
		file, err = os.OpenFile(filePath, os.O_WRONLY, 0666)
		if err != nil {
			return nil, err
		}
	} else if errors.Is(err, os.ErrNotExist) {
		Debug("File [%s] does not exist, creating...", filePath)
		file, err = os.Create(filePath)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}
	return file, nil
}

func openFileToRead(filePath string) (*os.File, error) {
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

func readFromFile(file *os.File, size uint64, offset uint64) ([]byte, error) {
	bytesBuffer := make([]byte, size)
	_, err := file.ReadAt(bytesBuffer, int64(offset))
	if err != nil {
		return nil, err
	}
	return bytesBuffer, nil
}

func writeToFile(file *os.File, bytes []byte, writeOffset int64) (int, error) {
	bytesWritten, err := file.WriteAt(bytes, writeOffset)
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

func Debug(format string, value1 string) {
	now := time.Now().Format(time.StampNano)
	format = "[DEBUG] " + now + ": " + format + "\n"
	fmt.Printf(format, value1)
}
