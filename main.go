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

	"github.com/google/uuid"
	"github.com/tjarratt/babble"
)

var indexMapKeyOrder = []string{}
var indexMap = map[string]IndexItem{}
var writeOffset uint64 = 0

const MapReferenceMaxSize = 100
const TestSize = 100000
const FilePathToData = "/tmp/dat2"

type KeyValueRow struct {
	Size  uint64
	Key   string
	Value string
}

type IndexItem struct {
	Offset uint64
	Size   uint64
}

func main() {
	var testData = map[string]string{}
	babbler := babble.NewBabbler()

	for i := 0; i < TestSize; i++ {
		savedKey := uuid.New().String()
		savedValue := babbler.Babble()
		testData[savedKey] = savedValue
	}

	for savedKey, savedValue := range testData {
		err := SaveKeyValueRow(savedKey, savedValue)
		if err != nil {
			panic(err)
		}
	}

	for savedKey, _ := range testData {
		retrievedValue, err := RetrieveValueForKey(savedKey)
		if err != nil {
			panic(err)
		}
		if retrievedValue != testData[savedKey] {
			panic("wrong retrieved_value!")
		}
	}

}

func SaveKeyValueRow(key string, value string) error {
	offset, bytesWritten, err := persist(key, value)
	if err != nil {
		return err
	}

	// Update index
	var indexItem IndexItem
	indexItem.Offset = offset
	indexItem.Size = bytesWritten
	indexMap[key] = indexItem
	indexMapKeyOrder = append(indexMapKeyOrder, key)
	writeOffset = writeOffset + bytesWritten

	// Sort if necessary
	if !slices.IsSorted(indexMapKeyOrder) {
		slices.Sort(indexMapKeyOrder)
	}

	return nil
}

func RetrieveValueForKey(key string) (string, error) {
	indexItem, valueExists := indexMap[key]
	if !valueExists {
		return "", errors.New("key not found")
	}

	segment, err := retrieve(indexItem.Offset, indexItem.Size)
	if err != nil {
		return "", err
	}

	return segment.Value, nil
}

// ---------------------------------------------------------------------------------------------------------------------
// DATA OPERATIONS
// ---------------------------------------------------------------------------------------------------------------------

func persist(key string, value string) (uint64, uint64, error) {
	file, err := openFileToWrite(FilePathToData)
	if err != nil {
		return 0, 0, err
	}

	keyValueRow := new(KeyValueRow)
	keyValueRow.Key = key
	keyValueRow.Value = value

	encodedBytes, err := encodeKeyValueRow(keyValueRow)
	if err != nil {
		return 0, 0, err
	}

	bytesWritten, err := writeToFile(file, encodedBytes)
	if err != nil {
		return 0, 0, err
	}

	return writeOffset, uint64(bytesWritten), nil
}

func retrieve(offset uint64, size uint64) (*KeyValueRow, error) {
	file, err := openFileToRead(FilePathToData)
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

func encodeKeyValueRow(row *KeyValueRow) ([]byte, error) {
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

func decodeKeyValueRow(bytesFromFile []byte) (*KeyValueRow, error) {
	var bytesBuffer bytes.Buffer
	var keyValueRow KeyValueRow

	stringBytes, err := base64.StdEncoding.DecodeString(string(bytesFromFile))
	if err != nil {
		return nil, err
	}

	bytesBuffer.Write(stringBytes)
	decoder := gob.NewDecoder(&bytesBuffer)
	err = decoder.Decode(&keyValueRow)
	if err != nil {
		return nil, err
	}

	return &keyValueRow, nil
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
		Debug1("File [%s] does not exist, creating...", filePath)
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

func writeToFile(file *os.File, bytes []byte) (int, error) {
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

func Debug0(format string) {
	Debug(format, "", "", "")
}

func Debug1(format string, value string) {
	Debug(format, value, "", "")
}

func Debug2(format string, value1 string, value2 string) {
	Debug(format, value1, value2, "")
}

func Debug3(format string, value1 string, value2 string, value3 string) {
	Debug(format, value1, value2, value3)
}

func Debug(format string, value1 string, value2 string, value3 string) {
	now := time.Now().Format(time.StampNano)
	format = "[DEBUG] " + now + ": " + format + "\n"

	if value1 == "" && value2 == "" && value3 == "" {
		fmt.Print(format)
		return
	}

	if value1 != "" && value2 == "" && value3 == "" {
		fmt.Printf(format, value1)
		return
	}

	if value1 != "" && value2 != "" && value3 == "" {
		fmt.Printf(format, value1, value2)
		return
	}

	if value1 != "" && value2 != "" && value3 != "" {
		fmt.Printf(format, value1, value2, value3)
		return
	}

}
