package segment

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"

	"github.com/c3r/data-storage-engine/files"
	"github.com/google/uuid"
)

const FilePathTemplate = "/tmp/tb_storage_%s"

type indexItem struct {
	Offset uint64
	Size   uint64
}

type segment struct {
	id       string
	filePath string
	offset   uint64
	syncMap  sync.Map //map[string]*indexItem
	mutex    sync.Mutex
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
		filePath: fmt.Sprintf(FilePathTemplate, id),
		syncMap:  sync.Map{},
		mutex:    sync.Mutex{},
	}
	for _, key := range order {
		value, _ := rows.Load(key)
		row := &segmentRow{
			Key:   key,
			Value: value.(string),
		}
		file, err := files.OpenFileWrite(segment.filePath)
		if err != nil {
			return err
		}
		bytes, err := encode(row)
		if err != nil {
			return err
		}
		bytesWritten, err := files.Write(file, bytes, segment.offset)
		if err != nil {
			return err
		}
		// After saving to file, update segment in memory
		segment.syncMap.Store(key, &indexItem{
			Offset: segment.offset,
			Size:   bytesWritten,
		})
		segment.offset += bytesWritten // atomic?
	}
	table.segments = append(table.segments, segment)
	return nil
}

func (segmentTable *SegmentTable) Load(key string) (string, error) {
	// This should be from the newest to oldest -> the newest value is saved later
	for _, segment := range segmentTable.segments {
		if item, valueExists := segment.syncMap.Load(key); valueExists {

			file, err := files.OpenFileRead(segment.filePath)
			if err != nil {
				return "", err
			}
			bytes, err := files.Read(file, item.(*indexItem).Size, item.(*indexItem).Offset)
			if err != nil {
				return "", err
			}
			row, err := decode(bytes)
			if err != nil {
				return "", err
			}
			return row.Value, nil
		}
	}
	e := fmt.Errorf("key %s not found", key)
	return "", errors.New(e.Error())
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
