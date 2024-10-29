package segment

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io/fs"
	"log"
	"os"
	"slices"
	"sync"

	"github.com/c3r/data-storage-engine/files"
	"github.com/c3r/data-storage-engine/syncmap"
)

const M_LEN = 8
const S_LVL = 2

type (
	OffsetIndex = *syncmap.Ordered[string, int64]
	Data        = *sync.Map
)

type Segment struct {
	Id           int64
	filePath     string
	headerLength int64
	fileSize     int64
	index        OffsetIndex
	wg           sync.WaitGroup
}

type persistentRow struct {
	Key   string
	Value string
}

func CreateSegment(id int64, data Data, dir string) (*Segment, error) {
	var bytesWritten, rowNum int
	var err error
	var row []byte
	var file *os.File
	var filePath string
	if filePath, err = files.Create(dir); err != nil {
		return nil, err
	}
	s := &Segment{
		Id:           id,
		filePath:     filePath,
		fileSize:     int64(0),
		headerLength: int64(0),
		index:        syncmap.New[string, int64](),
	}
	encode := func(segmentRow *persistentRow) ([]byte, error) {
		buf := bytes.Buffer{}
		encoder := gob.NewEncoder(&buf)
		if err := encoder.Encode(segmentRow); err != nil {
			return nil, err
		}
		encr := base64.StdEncoding.EncodeToString(buf.Bytes())
		encodedBytes := []byte(encr)
		return encodedBytes, nil
	}
	write_int := func(value int64) error {
		buf := make([]byte, M_LEN)
		binary.LittleEndian.PutUint64(buf, uint64(value))
		bytesWritten, err = file.WriteAt(buf, s.fileSize)
		s.fileSize += int64(bytesWritten)
		return err
	}
	write_data := func(data []byte) error {
		bytesWritten, err = file.WriteAt(data, s.fileSize)
		s.fileSize += int64(bytesWritten)
		return err
	}
	index_store := func(key string, value int64) {
		if rowNum%S_LVL == 0 {
			s.index.Store(key, value)
		}
		rowNum++
	}
	if file, err = files.OpenFileWrite(s.filePath); err != nil {
		return nil, err
	}
	if err = write_int(s.Id); err != nil {
		return nil, err
	}
	s.headerLength += M_LEN
	var order []string
	data.Range(func(key, value any) bool {
		order = append(order, key.(string))
		return true
	})
	slices.Sort(order)
	for _, k := range order {
		v, _ := data.Load(k)
		if row, err = encode(&persistentRow{k, v.(string)}); err != nil {
			break
		}
		length := int64(len(row))
		if err = write_int(length); err != nil {
			break
		}
		if err = write_data(row); err != nil {
			break
		}
		index_store(k, s.fileSize-length-M_LEN)
	}
	if err == nil {
		if err = files.Close(file); err == nil {
			return s, nil
		}
	}
	return nil, err
}

func (s *Segment) Size() int64 {
	return s.index.Size()
}

// Compaction - start
func (s *Segment) Merge(other *Segment, dir string) (*Segment, error) {
	var rows, rowsOther []*persistentRow
	var err error
	var it1, it2 int
	loadRowsFromFile := func(s *Segment) ([]*persistentRow, error) {
		file, _, err := files.OpenFileRead(s.filePath)
		if err != nil {
			return nil, err
		}
		offset := s.headerLength
		var rows []*persistentRow
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
	if rows, err = loadRowsFromFile(s); err != nil {
		return nil, err
	}
	if rowsOther, err = loadRowsFromFile(other); err != nil {
		return nil, err
	}
	result := &sync.Map{}
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
	return CreateSegment(other.Id, result, dir)
}

// Compaction - stop

func LoadPersistedSegments(dir string) ([]*Segment, error) {
	var err error
	var segments []*Segment
	var dirEntries []fs.DirEntry
	loadSegmentFromFile := func(filePath string) (*Segment, error) {
		var file *os.File
		var fileInfo os.FileInfo
		var offset int64
		var bytesRead uint64
		var row *persistentRow
		s := &Segment{
			filePath: filePath,
			index:    syncmap.New[string, int64](),
		}
		if file, fileInfo, err = files.OpenFileRead(filePath); err != nil {
			return nil, err
		}
		if s.Id, err = readField(file, uint64(offset)); err != nil {
			return nil, err
		}
		offset += M_LEN
		for offset < fileInfo.Size() {
			if row, bytesRead, err = readRowFromFile(file, uint64(offset)); err != nil {
				return nil, err
			}
			s.index.Store(row.Key, offset) // TODO: sparse table - don't store everything in memory!
			offset += int64(bytesRead)
		}
		files.Close(file)
		return s, nil
	}
	if dirEntries, err = os.ReadDir(dir); err != nil {
		return nil, err
	}
	for _, entry := range dirEntries {
		var s *Segment
		if !entry.Type().IsRegular() {
			continue
		}
		filePath := fmt.Sprintf("%s/%s", dir, entry.Name())
		if s, err = loadSegmentFromFile(filePath); err != nil {
			log.Printf("Cannot load from file %s: %s", filePath, err.Error())
		}
		segments = append(segments, s)
	}
	return segments, nil
}

func (s *Segment) Delete() error {
	s.wg.Wait()
	return files.Delete(s.filePath)
}

func (s *Segment) Load(key string) (string, bool, error) {
	find := func(dataKey string) (int64, int64) {
		var startKey, stopKey string
		var start, stop int64
		var valueExists bool
		s.index.ForKeys(func(_key string) bool {
			if dataKey > _key {
				startKey = _key
				return true
			}
			if dataKey < _key {
				stopKey = _key
				return false
			}
			return true
		})
		if start, valueExists = s.index.Load(startKey); !valueExists {
			start = s.headerLength
		}
		if stop, valueExists = s.index.Load(stopKey); !valueExists {
			stop = s.fileSize
		}
		return start, stop
	}
	s.wg.Add(1)
	defer s.wg.Done()
	// Just open the file, we will be reading from it for sure
	file, fileInfo, err := files.OpenFileRead(s.filePath)
	if err != nil {
		return "", false, err
	}
	defer files.Close(file)
	var stop, offset int64
	var valueExists bool
	if offset, valueExists = s.index.Load(key); valueExists {
		stop = offset
	} else {
		offset, stop = find(key)
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

// Private
func readRowFromFile(file *os.File, offset uint64) (*persistentRow, uint64, error) {
	decode := func(bytesFromFile []byte) (*persistentRow, error) {
		var buf bytes.Buffer
		var row persistentRow
		bytes, err := base64.StdEncoding.DecodeString(string(bytesFromFile))
		if err != nil {
			return nil, err
		}
		buf.Write(bytes)
		decoder := gob.NewDecoder(&buf)
		err = decoder.Decode(&row)
		if err != nil {
			return nil, err
		}
		return &row, nil
	}
	var row *persistentRow
	var err error
	var dataLen int64
	var dataBytes []byte
	if dataLen, err = readField(file, offset); err != nil {
		return nil, 0, err
	}
	if dataBytes, err = files.Read(file, uint64(dataLen), offset+M_LEN); err != nil {
		return nil, 0, err
	}
	if row, err = decode(dataBytes); err != nil {
		return nil, 0, err
	}
	return row, uint64(dataLen) + M_LEN, nil
}

func readField(file *os.File, offset uint64) (int64, error) {
	var buf []byte
	var err error
	if buf, err = files.Read(file, M_LEN, offset); err == nil {
		return int64(binary.LittleEndian.Uint64(buf)), nil
	}
	return 0, err
}
