package segment

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
	"os"
	"sync"

	"github.com/c3r/data-storage-engine/files"
	"github.com/c3r/data-storage-engine/syncmap"
)

const FILE_PATH_TEMPLATE = "/tmp/tb_storage_%s"
const METADATA_FIELD_SIZE = 8
const SEGMENT_SPARSE_TABLE_LVL = 2

type (
	OffsetIndex = *syncmap.Ordered[string, int64]
	Data        = *syncmap.Ordered[string, string]
)

type Segment struct {
	Id          int64
	FilePath    string
	offset      int64
	fileSize    int64
	offsetIndex OffsetIndex
	Wg          sync.WaitGroup
}

type PersistentRow struct {
	Key   string
	Value string
}

func CreateSegment(id int64, data Data, filePath string) (*Segment, error) {
	s := &Segment{
		Id:          id,
		FilePath:    filePath,
		fileSize:    int64(0),
		offset:      METADATA_FIELD_SIZE * 2,
		offsetIndex: syncmap.New[string, int64](),
	}
	s, err := s.writeSegment(data)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Segment) Size() int64 {
	return s.offsetIndex.Size()
}

// TODO: extract encode&save data to method
// Private
func (s *Segment) writeSegment(data Data) (*Segment, error) {
	// Open file for writing segment:
	file, err := files.OpenFileWrite(s.FilePath)
	if err != nil {
		return nil, err
	}
	// Encode and save the segment id:
	encodedId := make([]byte, METADATA_FIELD_SIZE)
	binary.LittleEndian.PutUint64(encodedId, uint64(s.Id))
	bytesWritten, err := file.WriteAt(encodedId, s.fileSize)
	if err != nil {
		return nil, err
	}
	s.fileSize += int64(bytesWritten)
	// Encode and save the segment size:
	encodedSize := make([]byte, METADATA_FIELD_SIZE)
	binary.LittleEndian.PutUint64(encodedSize, uint64(s.Id))
	bytesWritten, err = file.WriteAt(encodedSize, s.fileSize)
	if err != nil {
		return nil, err
	}
	s.fileSize += int64(bytesWritten)
	// For every row:
	// encode and save the row with it's length:
	var e error
	var idx int
	data.Entries(func(key, value string) bool {
		// Encode data:
		encodedRow, err := encode(&PersistentRow{key, value})
		if err != nil {
			e = err
			return false
		}
		// Encode and save the length of the data:
		encodedRowLength := make([]byte, METADATA_FIELD_SIZE)
		binary.LittleEndian.PutUint64(encodedRowLength, uint64(len(encodedRow)))
		encodedRowLengthWriteOffset := s.fileSize //
		bytesWritten, err = file.WriteAt(encodedRowLength, s.fileSize)
		if err != nil {
			e = err
			return false
		}
		s.fileSize += int64(bytesWritten)
		// Save the encoded data:
		bytesWritten, err = file.WriteAt(encodedRow, s.fileSize)
		if err != nil {
			e = err
			return false
		}
		// After saving to file,
		// 	FIRST update segment in memory (save in sparse table) and
		//	SECOND increase the file size (write offset in this context):
		if idx%SEGMENT_SPARSE_TABLE_LVL == 0 {
			s.offsetIndex.Store(key, encodedRowLengthWriteOffset)
			// segment.index = append(segment.index, key)
		}
		s.fileSize += int64(bytesWritten)
		idx++
		return true
	})
	if e != nil {
		return nil, e
	}
	// Sync and close file
	err = file.Sync()
	if err != nil {
		return nil, err
	}
	err = file.Close()
	if err != nil {
		return nil, err
	}
	return s, nil
}

// TODO: merge loadSegmentFromFile and loadRowsFromFile?
func (s *Segment) LoadRowsFromFile() ([]*PersistentRow, error) {
	file, _, err := files.OpenFileRead(s.FilePath)
	if err != nil {
		return nil, err
	}
	offset := s.offset
	var rows []*PersistentRow
	for offset < s.fileSize {
		row, bytesRead, err := readRowFromFile(file, uint64(offset))
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
		offset += int64(bytesRead)
	}
	return rows, nil
}

// Private
func loadSegmentFromFile(filePath string) (*Segment, error) {
	segment := &Segment{
		FilePath:    filePath,
		offsetIndex: syncmap.New[string, int64](),
	}
	file, fileInfo, err := files.OpenFileRead(filePath)
	if err != nil {
		return nil, err
	}
	offset := int64(0)
	for offset < fileInfo.Size() {
		row, bytesRead, err := readRowFromFile(file, uint64(offset))
		if err != nil {
			return nil, err
		}
		segment.offsetIndex.Store(row.Key, offset) // TODO: sparse table - don't store everything in memory!
		offset += int64(bytesRead)
	}
	files.Close(file)
	return segment, nil
}

func (s *Segment) Wait() {
	s.Wg.Wait()
}

func (s *Segment) Load(key string) (string, bool, error) {
	s.Wg.Add(1)
	defer s.Wg.Done()
	// Just open the file, we will be reading from it for sure
	file, fileInfo, err := files.OpenFileRead(s.FilePath)
	if err != nil {
		return "", false, err
	}
	defer files.Close(file)
	var stop, offset int64
	var valueExists bool
	if offset, valueExists = s.offsetIndex.Load(key); valueExists {
		stop = offset
	} else {
		offset, stop = s.findNearestOffset(key)
	}
	for offset < fileInfo.Size() && offset <= stop {
		row, bytesRead, err := readRowFromFile(file, uint64(offset))
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

func (segment *Segment) Merge(other *Segment, filePath string) (*Segment, error) {
	rows, err := segment.LoadRowsFromFile()
	if err != nil {
		return nil, err
	}
	rowsOther, err := other.LoadRowsFromFile()
	if err != nil {
		return nil, err
	}
	result := syncmap.New[string, string]()
	it1 := 0
	it2 := 0
	for it2 <= len(rows)-1 && it1 <= len(rowsOther)-1 {
		if rows[it1].Key == rowsOther[it2].Key {
			result.Store(rowsOther[it2].Key, rowsOther[it2].Value)
			it2++
			it1++
		} else if rows[it1].Key < rowsOther[it2].Key {
			result.Store(rows[it1].Key, rows[it1].Value)
			it1++
		} else {
			result.Store(rowsOther[it2].Key, rowsOther[it2].Value)
			it2++
		}
		if it1 == len(rows) {
			for _, row := range rowsOther[it2:] {
				result.Store(row.Key, row.Value)
			}
		} else if it2 == len(rowsOther) {
			for _, row := range rows[it1:] {
				result.Store(row.Key, row.Value)
			}
		}
	}
	merged, err := CreateSegment(other.Id, result, filePath)

	return merged, err
}

// Private
func (s *Segment) findNearestOffset(dataKey string) (int64, int64) {
	var startKey, stopKey string
	var start, stop int64
	var valueExists bool
	s.offsetIndex.Keys(func(key string) bool {
		if dataKey > key {
			startKey = key
			return false
		}
		if dataKey < key {
			stopKey = key
		}
		return false
	})
	if start, valueExists = s.offsetIndex.Load(startKey); !valueExists {
		start = s.offset
	}
	if stop, valueExists = s.offsetIndex.Load(stopKey); !valueExists {
		stop = s.fileSize
	}
	return start, stop
}

// Private1
// Assumptions:
//   - file is open and will be closed
//   - offset < fileSize
func readRowFromFile(file *os.File, offset uint64) (*PersistentRow, uint64, error) {
	dataLenBytes, err := files.Read(file, METADATA_FIELD_SIZE, offset)
	if err != nil {
		return nil, 0, err
	}
	dataLen := binary.LittleEndian.Uint64(dataLenBytes)
	bytes, err := files.Read(file, dataLen, offset+METADATA_FIELD_SIZE)
	if err != nil {
		return nil, 0, err
	}
	row, err := decode(bytes)
	if err != nil {
		return nil, 0, err
	}
	return row, dataLen + METADATA_FIELD_SIZE, nil
}

// Private
func encode(segmentRow *PersistentRow) ([]byte, error) {
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

// Private
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
