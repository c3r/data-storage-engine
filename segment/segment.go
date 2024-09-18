package segment

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"fmt"

	"github.com/c3r/data-storage-engine/files"
)

const FilePathTemplate = "/tmp/tb_storage_%d"

type SegmentIndexItem struct {
	Offset uint64
	Size   uint64
}

type Segment struct {
	Id       int
	FilePath string
	Offset   uint64
	Index    map[string]*SegmentIndexItem
}

type SegmentRow struct {
	Key   string
	Value string
}

type SegmentTable struct {
	segments []*Segment
}

func (table *SegmentTable) Append(segment *Segment) {
	table.segments = append(table.segments, segment)
}

func (table *SegmentTable) Length() int {
	return len(table.segments)
}

func (table *SegmentTable) Get(key int) *Segment {
	return table.segments[key]
}

func (seg *Segment) Write(id int, order []string, table map[string]string) error {
	seg.Id = id
	seg.Index = make(map[string]*SegmentIndexItem)
	seg.FilePath = fmt.Sprintf(FilePathTemplate, id)
	for _, key := range order {
		row := new(SegmentRow)
		row.Key = key
		row.Value = table[key]
		bytes, err := encodeRow(row)
		if err != nil {
			return err
		}
		file, err := files.OpenFileWrite(seg.FilePath)
		if err != nil {
			return err
		}
		bytesWritten, err := files.Write(file, bytes, seg.Offset)
		if err != nil {
			return err
		}
		// After saving to file, update segment in memory
		var item SegmentIndexItem
		item.Offset = seg.Offset
		item.Size = bytesWritten
		seg.Index[key] = &item
		seg.Offset += bytesWritten
	}
	return nil
}

func (seg *Segment) Read(key string) ([]byte, bool, error) {
	item, valueExists := seg.Index[key]
	if !valueExists {
		return nil, false, nil
	}
	file, err := files.OpenFileRead(seg.FilePath)
	if err != nil {
		return nil, false, err
	}
	bytesFromFile, err := files.Read(file, item.Size, item.Offset)
	if err != nil {
		return nil, false, err
	}
	return bytesFromFile, true, nil
}

func encodeRow(row *SegmentRow) ([]byte, error) {
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

func DecodeRow(bytesFromFile []byte) (*SegmentRow, error) {
	var bytesBuffer bytes.Buffer
	var row SegmentRow
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
