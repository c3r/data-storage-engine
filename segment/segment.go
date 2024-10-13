package segment

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"

	"github.com/c3r/data-storage-engine/files"
	"github.com/c3r/data-storage-engine/syncmap"
	"github.com/google/uuid"
)

const FILE_PATH_TEMPLATE = "/tmp/tb_storage_%s"
const METADATA_FIELD_SIZE = 8
const SEGMENT_SPARSE_TABLE_LVL = 5

type Segment struct {
	Id         int64
	RowsNumber int64
	FilePath   string
	DataOffset int64
	FileSize   int64
	SyncMap    *syncmap.SynchronizedMap[string, int64]
	Index      []string
}

type segmentRow struct {
	Key   string
	Value string
}

type SegmentTable struct {
	segmentsMap   *syncmap.SynchronizedMap[int64, *Segment]
	segmentsIndex []int64
	lock          sync.Mutex
}

func New() *SegmentTable {
	return &SegmentTable{syncmap.New[int64, *Segment](), []int64{}, sync.Mutex{}}
}

func (table *SegmentTable) PersistToFile(threadId int, id int64, order []string, rows *syncmap.SynchronizedMap[string, string], dirPath string) (string, error) {
	segment, err := table.writeSegment(id, order, rows, dirPath)
	if err != nil {
		return "", err
	}
	table.insert(segment, threadId)
	return segment.FilePath, nil
}

func (table *SegmentTable) Load(key string) (string, error) {
	// Iterating from the newest to oldest: the newest value is saved later
	for segmentKey := len(table.segmentsIndex) - 1; segmentKey >= 0; segmentKey-- {
		idx := table.segmentsIndex[segmentKey]
		segment, _ := table.segmentsMap.Load(idx)
		value, valueExists, err := segment.search(key)
		if err != nil {
			return "", err
		}
		if valueExists {
			return value, nil
		}
	}
	err := fmt.Errorf("key %s not found", key)
	return "", errors.New(err.Error())
}

func (table *SegmentTable) LoadSegmentsFromDisk(threadId int, segmentFilesDirectory string) error {
	directoryEntries, err := os.ReadDir(segmentFilesDirectory)
	if err != nil {
		return err
	}
	for _, directoryEntry := range directoryEntries {
		if !directoryEntry.Type().IsRegular() {
			continue
		}
		filePath := fmt.Sprintf("%s/%s", segmentFilesDirectory, directoryEntry.Name())
		fmt.Printf("Loading segment from file %s...", filePath)
		segment, err := loadSegmentFromFile(filePath)
		if err != nil {
			return err
		}
		// Lock
		table.insert(segment, threadId)
		// Unlock
	}
	return nil
}

func (table *SegmentTable) Compact(threadId int, segmentsDirPath string) error {
	if len(table.segmentsIndex) < 2 {
		// Nothing to compact
		return nil
	}
	// LOCK
	older, _ := table.segmentsMap.Load(table.segmentsIndex[len(table.segmentsIndex)-2])
	newer, _ := table.segmentsMap.Load(table.segmentsIndex[len(table.segmentsIndex)-1])
	// fmt.Printf("newer: %d, older: %d\n", newer.Id, older.Id)
	if newer.RowsNumber != older.RowsNumber {
		// Nothing to compact
		return nil
	}
	// UNLOCK
	rowsOlder, err := older.loadRowsFromFile()
	if err != nil {
		return err
	}
	rowsNewer, err := newer.loadRowsFromFile()
	if err != nil {
		return err
	}
	rowMap, order := mergeSortRows(rowsOlder, rowsNewer)
	merged, err := table.writeSegment(newer.Id, order, rowMap, segmentsDirPath)
	if err != nil {
		return err
	}
	// Lock
	// Remove last two and insert the new segment:
	// fmt.Printf("Removing segments %d and %d...", older.Id, newer.Id)
	table.remove(older.Id)
	table.remove(newer.Id)
	// fmt.Printf("Inserting segment %d...", merged.Id)
	table.insert(merged, threadId)
	// Unlock
	if err := files.Delete(older.FilePath); err != nil {
		// TODO: add error and try later
	}
	if err := files.Delete(newer.FilePath); err != nil {
		// TODO: add error and try later
	}
	return nil
}

// TODO: extract encode&save data to method
// Private
func (table *SegmentTable) writeSegment(id int64, order []string, rows *syncmap.SynchronizedMap[string, string], dirPath string) (*Segment, error) {
	filePath := fmt.Sprintf(dirPath + "/" + uuid.New().String()) // TODO: handle slash ending
	segment := &Segment{
		Id:         id,
		RowsNumber: int64(len(order)),
		FilePath:   filePath,
		FileSize:   int64(0),
		DataOffset: METADATA_FIELD_SIZE * 2,
		SyncMap:    syncmap.New[string, int64](),
	}
	// Open file for writing segment:
	file, err := files.OpenFileWrite(segment.FilePath)
	if err != nil {
		return nil, err
	}
	// Encode and save the segment id:
	encodedId := make([]byte, METADATA_FIELD_SIZE)
	binary.LittleEndian.PutUint64(encodedId, uint64(segment.Id))
	bytesWritten, err := file.WriteAt(encodedId, segment.FileSize)
	if err != nil {
		return nil, err
	}
	segment.FileSize += int64(bytesWritten)
	// Encode and save the segment size:
	encodedSize := make([]byte, METADATA_FIELD_SIZE)
	binary.LittleEndian.PutUint64(encodedSize, uint64(segment.Id))
	bytesWritten, err = file.WriteAt(encodedSize, segment.FileSize)
	if err != nil {
		return nil, err
	}
	segment.FileSize += int64(bytesWritten)
	// For every row:
	// encode and save the row with it's length:
	for idx, key := range order {
		value, _ := rows.Load(key) // TODO: error handling
		// Encode data:
		encodedRow, err := encodeRow(&segmentRow{key, value})
		if err != nil {
			return nil, err
		}
		// Encode and save the length of the data:
		encodedRowLength := make([]byte, METADATA_FIELD_SIZE)
		binary.LittleEndian.PutUint64(encodedRowLength, uint64(len(encodedRow)))
		encodedRowLengthWriteOffset := segment.FileSize //
		bytesWritten, err = file.WriteAt(encodedRowLength, segment.FileSize)
		if err != nil {
			return nil, err
		}
		segment.FileSize += int64(bytesWritten)
		// Save the encoded data:
		bytesWritten, err = file.WriteAt(encodedRow, segment.FileSize)
		if err != nil {
			return nil, err
		}
		// After saving to file,
		// 	FIRST update segment in memory (save in sparse table) and
		//	SECOND increase the file size (write offset in this context):
		if idx%SEGMENT_SPARSE_TABLE_LVL == 0 {
			segment.SyncMap.Store(key, encodedRowLengthWriteOffset)
			segment.Index = append(segment.Index, key)
		}
		segment.FileSize += int64(bytesWritten)
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
	slices.Sort(segment.Index)
	return segment, nil
}

// Private
func (table *SegmentTable) insert(segment *Segment, threadId int) {
	table.lock.Lock()
	table.segmentsIndex = append(table.segmentsIndex, segment.Id)
	slices.Sort(table.segmentsIndex)
	table.segmentsMap.Store(segment.Id, segment)
	table.lock.Unlock()
}

// Private
func (table *SegmentTable) remove(segmentKeyToRemove int64) {
	table.lock.Lock()
	table.segmentsMap.Delete(segmentKeyToRemove)
	for idx, segmentKey := range table.segmentsIndex {
		if segmentKeyToRemove == int64(segmentKey) {
			table.segmentsIndex = append(table.segmentsIndex[:idx], table.segmentsIndex[idx+1:]...)
			break
		}
	}
	table.lock.Unlock()
}

// Private
func mergeSortRows(older []*segmentRow, newer []*segmentRow) (*syncmap.SynchronizedMap[string, string], []string) {
	result := syncmap.New[string, string]()
	var index []string
	idxOlder := 0
	idxNewer := 0
	for idxNewer <= len(older)-1 && idxOlder <= len(newer)-1 {
		if older[idxOlder].Key == newer[idxNewer].Key {
			result.Store(newer[idxNewer].Key, newer[idxNewer].Value)
			index = append(index, newer[idxNewer].Key)
			idxNewer++
			idxOlder++
		} else if older[idxOlder].Key < newer[idxNewer].Key {
			result.Store(older[idxOlder].Key, older[idxOlder].Value)
			index = append(index, older[idxOlder].Key)
			idxOlder++
		} else {
			result.Store(newer[idxNewer].Key, newer[idxNewer].Value)
			index = append(index, newer[idxNewer].Key)
			idxNewer++
		}
		if idxOlder == len(older) {
			for _, row := range newer[idxNewer:] {
				result.Store(row.Key, row.Value)
				index = append(index, row.Key)
			}
		} else if idxNewer == len(newer) {
			for _, row := range older[idxOlder:] {
				result.Store(row.Key, row.Value)
				index = append(index, row.Key)
			}
		}
	}
	return result, index
}

// Private
// TODO: merge loadSegmentFromFile and loadRowsFromFile?
func (segment *Segment) loadRowsFromFile() ([]*segmentRow, error) {
	file, _, err := files.OpenFileRead(segment.FilePath)
	if err != nil {
		return nil, err
	}
	offset := segment.DataOffset
	var rows []*segmentRow
	for offset < segment.FileSize {
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
		FilePath: filePath,
		SyncMap:  syncmap.New[string, int64](),
	}
	file, fileInfo, err := files.OpenFileRead(filePath)
	if err != nil {
		return nil, err
	}
	offset := int64(0)
	rowCount := 0
	for offset < fileInfo.Size() {
		row, bytesRead, err := readRowFromFile(file, uint64(offset))
		if err != nil {
			return nil, err
		}
		segment.SyncMap.Store(row.Key, offset) // TODO: sparse table - don't store everything in memory!
		segment.Index = append(segment.Index, row.Key)
		offset += int64(bytesRead)
		rowCount++
	}
	slices.Sort(segment.Index)
	files.Close(file)
	return segment, nil
}

// Private
func (segment *Segment) search(key string) (string, bool, error) {
	// Just open the file, we will be reading from it for sure
	file, fileInfo, err := files.OpenFileRead(segment.FilePath)
	if err != nil {
		return "", false, err
	}
	defer files.Close(file)
	var stop, offset int64
	var valueExists bool
	if offset, valueExists = segment.SyncMap.Load(key); valueExists {
		stop = offset
	} else {
		offset, stop = segment.findNearestOffset(key)
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
func (segment *Segment) findNearestOffset(key string) (int64, int64) {
	var keyStart, keyStop string
	for _, otherKey := range segment.Index {
		if key > otherKey {
			keyStart = otherKey
			continue
		}
		if key < otherKey {
			keyStop = otherKey
			break
		}
	}
	start, startExists := segment.SyncMap.Load(keyStart)
	if !startExists {
		start = segment.DataOffset
	}
	stop, stopExists := segment.SyncMap.Load(keyStop)
	if !stopExists {
		stop = segment.FileSize
	}
	return start, stop
}

// Private
// Assumptions:
//   - file is open and will be closed
//   - offset < fileSize
func readRowFromFile(file *os.File, offset uint64) (*segmentRow, uint64, error) {
	dataLenBytes, err := files.Read(file, METADATA_FIELD_SIZE, offset)
	if err != nil {
		return nil, 0, err
	}
	dataLen := binary.LittleEndian.Uint64(dataLenBytes)
	bytes, err := files.Read(file, dataLen, offset+METADATA_FIELD_SIZE)
	if err != nil {
		return nil, 0, err
	}
	row, err := decodeSegment(bytes)
	if err != nil {
		return nil, 0, err
	}
	return row, dataLen + METADATA_FIELD_SIZE, nil
}

// Private
func encodeRow(segmentRow *segmentRow) ([]byte, error) {
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
func decodeSegment(bytesFromFile []byte) (*segmentRow, error) {
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
