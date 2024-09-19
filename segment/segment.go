package segment

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/c3r/data-storage-engine/files"
)

const FilePathTemplate = "/tmp/tb_storage_%s"

type indexItem struct {
	Offset uint64
	Size   uint64
}

type segment struct {
	Id       string
	FilePath string
	Offset   uint64
	Index    map[string]*indexItem
}

type segmentRow struct {
	Key   string
	Value string
}

type SegmentTable struct {
	segments []*segment
}

func (st *SegmentTable) Create(id string, order []string, table map[string]string) error {
	s := &segment{
		Id:       id,
		FilePath: fmt.Sprintf(FilePathTemplate, id),
		Index:    make(map[string]*indexItem),
	}
	st.segments = append(st.segments, s)
	for _, key := range order {
		row := &segmentRow{
			Key:   key,
			Value: table[key],
		}
		bytes, err := encode(row)
		if err != nil {
			return err
		}
		file, err := files.OpenFileWrite(s.FilePath)
		if err != nil {
			return err
		}
		bytesWritten, err := files.Write(file, bytes, s.Offset)
		if err != nil {
			return err
		}
		// After saving to file, update segment in memory
		var item indexItem
		item.Offset = s.Offset
		item.Size = bytesWritten
		s.Index[key] = &item
		s.Offset += bytesWritten
	}
	return nil
}

func (st *SegmentTable) Load(key string) (string, error) {
	for _, s := range st.segments {
		bytes, valueExists, err := s.read(key)
		if err != nil {
			return "", err
		}
		if valueExists {
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

func (s *segment) read(key string) ([]byte, bool, error) {
	item, valueExists := s.Index[key]
	if !valueExists {
		return nil, false, nil
	}
	file, err := files.OpenFileRead(s.FilePath)
	if err != nil {
		return nil, false, err
	}
	bytesFromFile, err := files.Read(file, item.Size, item.Offset)
	if err != nil {
		return nil, false, err
	}
	return bytesFromFile, true, nil
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
