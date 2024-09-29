package segment

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"sync"

	"github.com/c3r/data-storage-engine/files"
	"github.com/google/uuid"
)

const FILE_PATH_TEMPLATE = "/tmp/tb_storage_%s"
const DATA_LENGHT_BYTES_SIZE = 8
const SEGMENT_SPARSE_TABLE_LVL = 10

var logger log.Logger = *log.Default()

type indexItem struct {
	Offset int64
}

type segment struct {
	id       string
	filePath string
	offset   int64
	syncMap  sync.Map
	index    []string
}

type segmentRow struct {
	Key   string
	Value string
}

type SegmentTable struct {
	segments []*segment
}

func (table *SegmentTable) Create(order []string, rows *sync.Map) error {
	id := uuid.New().String()
	segment := &segment{
		id:       id,
		filePath: fmt.Sprintf(FILE_PATH_TEMPLATE, id),
		syncMap:  sync.Map{},
	}
	for idx, key := range order {
		value, _ := rows.Load(key)
		row := &segmentRow{
			Key:   key,
			Value: value.(string),
		}
		file, err := files.OpenFileWrite(segment.filePath)
		if err != nil {
			return err
		}
		dataRowBytes, err := encode(row)
		if err != nil {
			return err
		}
		// First save the length of the data
		dataLenBytes := make([]byte, DATA_LENGHT_BYTES_SIZE)
		binary.LittleEndian.PutUint64(dataLenBytes, uint64(len(dataRowBytes)))
		bytesWritten, err := file.WriteAt(dataLenBytes, segment.offset)
		if err != nil {
			return err
		}
		bytesWritten, err = file.WriteAt(dataRowBytes, segment.offset+int64(bytesWritten))
		if err != nil {
			return err
		}
		err = file.Sync()
		if err != nil {
			return err
		}
		err = file.Close()
		if err != nil {
			return err
		}
		// After saving to file, update segment in memory
		// Save in sparse table
		if idx%SEGMENT_SPARSE_TABLE_LVL == 0 {
			segment.syncMap.Store(key, segment.offset)
			segment.index = append(segment.index, key)
		}
		segment.offset += int64(bytesWritten) + DATA_LENGHT_BYTES_SIZE
	}
	slices.Sort(segment.index)
	table.segments = append(table.segments, segment)
	return nil
}

func (table *SegmentTable) Load(key string) (string, error) {
	// This should be from the newest to oldest -> the newest value is saved later
	for _, segment := range table.segments {
		if value, valueExists, err := segment.load(key); err == nil {
			if valueExists {
				return value, nil
			}
		} else {
			return "", err
		}
	}
	e := fmt.Errorf("key %s not found", key)
	return "", errors.New(e.Error())
}

func (segment *segment) load(key string) (string, bool, error) {
	// Just open the file, we will be reading from it for sure
	file, fileInfo, err := files.OpenFileRead(segment.filePath)
	if err != nil {
		return "", false, err
	}
	offset, valueExists := segment.loadFromMap(key)
	stop := offset
	if !valueExists {
		var startExists, stopExists bool
		offset, stop, startExists, stopExists = segment.search(key)
		if !startExists {
			offset = int64(0)
		}
		if !stopExists {
			stop = fileInfo.Size()
		}
	}
	for offset < fileInfo.Size() && offset <= stop {
		row, bytesRead, err := read(file, uint64(offset))
		if err != nil {
			return "", false, err
		}
		if row.Key == key {
			return row.Value, true, nil
		}
		offset += int64(bytesRead)
	}
	return "", false, nil
}

func (segment *segment) search(key string) (int64, int64, bool, bool) {
	var keyStart, keyStop string
	for _, other := range segment.index {
		if key > other {
			keyStart = other
			continue
		}
		if key < other {
			keyStop = other
			break
		}
	}
	start, startExists := segment.loadFromMap(keyStart)
	stop, stopExists := segment.loadFromMap(keyStop)
	return start, stop, startExists, stopExists
}

func (segment *segment) loadFromMap(key string) (int64, bool) {
	if offset, valueExists := segment.syncMap.Load(key); valueExists {
		return offset.(int64), true
	}
	return 0, false
}

func read(file *os.File, offset uint64) (*segmentRow, uint64, error) {
	dataLenBytes, err := files.Read(file, DATA_LENGHT_BYTES_SIZE, offset)
	if err != nil {
		return nil, 0, err
	}
	dataLen := binary.LittleEndian.Uint64(dataLenBytes)
	bytes, err := files.Read(file, dataLen, offset+DATA_LENGHT_BYTES_SIZE)
	if err != nil {
		return nil, 0, err
	}
	row, err := decode(bytes)
	if err != nil {
		return nil, 0, err
	}
	return row, dataLen + DATA_LENGHT_BYTES_SIZE, nil
}

func encode(segmentRow *segmentRow) ([]byte, error) {
	bytesBuffer := bytes.Buffer{}
	gobEncoder := gob.NewEncoder(&bytesBuffer)
	err := gobEncoder.Encode(segmentRow)
	if err != nil {
		return nil, err
	}
	encodedRow := base64.StdEncoding.EncodeToString(bytesBuffer.Bytes())
	encodedBytes := []byte(encodedRow)
	return encodedBytes, nil
}

func decode(bytesFromFile []byte) (*segmentRow, error) {
	var bytesBuffer bytes.Buffer
	var row segmentRow
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

// func (segmentTable *SegmentTable) compact() {
// 	if len(segmentTable.segments) >= 2 {
// 		s1 := segmentTable.segments[len(segmentTable.segments)-1]
// 		s2 := segmentTable.segments[len(segmentTable.segments)-2]
// 		fp1 := s1.filePath
// 		fp2 := s2.filePath
// 		file1, _ := files.OpenFileRead(fp1)
// 		file2, _ := files.OpenFileRead(fp2)

// 	}
// }
