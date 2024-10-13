package segment

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/c3r/data-storage-engine/files"
	"github.com/c3r/data-storage-engine/syncmap"
	"github.com/google/uuid"
)

const FILE_PATH_TEMPLATE = "/tmp/tb_storage_%s"
const METADATA_FIELD_SIZE = 8
const SEGMENT_SPARSE_TABLE_LVL = 5

type segment struct {
	id         int64
	rowsNumber int64
	filePath   string
	offset     int64
	fileSize   int64
	syncMap    *syncmap.SynchronizedMap[string, int64]
	index      []string
}

type persistentRow struct {
	Key   string
	Value string
}

type Segments struct {
	segmentsMap   *syncmap.SynchronizedMap[int64, *segment]
	segmentsIndex []int64
	mutex         sync.Mutex
}

func New() *Segments {
	return &Segments{
		segmentsMap:   syncmap.New[int64, *segment](),
		segmentsIndex: []int64{},
		mutex:         sync.Mutex{}}
}

func (table *Segments) PersistToFile(request *SegmentPersistToFileRequest) *SegmentPersistToFileResponse {
	segment, err := table.writeSegment(request.Id, request.Order, request.Rows, request.DirPath)
	if err != nil {
		return &SegmentPersistToFileResponse{
			Ok:      false,
			Message: "error while writing segment",
			Error:   err,
		}
	}
	table.insert(segment)
	return &SegmentPersistToFileResponse{
		Ok:      true,
		Message: fmt.Sprintf("saved %d rows at %s", segment.rowsNumber, segment.filePath),
		Error:   nil,
	}
}

func (table *Segments) LoadValue(key string) *SegmentLoadValueResponse {
	// Iterating from the newest to oldest: the newest value is saved later
	for segmentKey := len(table.segmentsIndex) - 1; segmentKey >= 0; segmentKey-- {
		idx := table.segmentsIndex[segmentKey]
		segment, _ := table.segmentsMap.Load(idx)
		value, valueExists, err := segment.search(key)
		if err != nil {
			return &SegmentLoadValueResponse{
				Ok:      false,
				Value:   "",
				Message: "error while searching within segment",
				Error:   err,
			}
		}
		if valueExists {
			return &SegmentLoadValueResponse{
				Ok:      true,
				Value:   value,
				Message: "value found",
				Error:   nil,
			}
		}
	}
	return &SegmentLoadValueResponse{
		Ok:      false,
		Value:   "",
		Message: "key not found",
		Error:   nil,
	}
}

func (table *Segments) LoadSegmentsFromDisk(request *SegmentLoadSegmentsFromDiskRequest) *SegmentLoadSegmentsFromDiskResponse {
	directoryEntries, err := os.ReadDir(request.DirPath)
	if err != nil {
		return &SegmentLoadSegmentsFromDiskResponse{
			Ok:      false,
			Message: "error while reading segment directory",
			Error:   err,
		}
	}
	for _, directoryEntry := range directoryEntries {
		if !directoryEntry.Type().IsRegular() {
			continue
		}
		filePath := fmt.Sprintf("%s/%s", request.DirPath, directoryEntry.Name())
		segment, err := loadSegmentFromFile(filePath)
		if err != nil {
			return &SegmentLoadSegmentsFromDiskResponse{
				Ok:      false,
				Message: "error while loading segment from file",
				Error:   err,
			}
		}
		// Lock?
		table.insert(segment)
		// Unlock?
	}
	return nil
}

func (table *Segments) CompactSegmentData(request *SegmentCompactionRequest) *SegmentCompactionResponse {
	if len(table.segmentsIndex) < 2 {
		return &SegmentCompactionResponse{
			Ok:       false,
			Duration: -1,
			Message:  "too few segments",
			Error:    nil,
		}
	}
	start := time.Now()
	older, _ := table.segmentsMap.Load(table.segmentsIndex[len(table.segmentsIndex)-2])
	newer, _ := table.segmentsMap.Load(table.segmentsIndex[len(table.segmentsIndex)-1])
	if newer.rowsNumber != older.rowsNumber {
		return &SegmentCompactionResponse{
			Ok:       false,
			Duration: -1,
			Message:  "two most recent segments has different number of rows",
			Error:    nil,
		}
	}
	rowsOlder, err := older.loadRowsFromFile()
	if err != nil {
		return &SegmentCompactionResponse{
			Ok:       false,
			Duration: -1,
			Message:  "error loading older rows",
			Error:    err,
		}
	}
	rowsNewer, err := newer.loadRowsFromFile()
	if err != nil {
		return &SegmentCompactionResponse{
			Ok:       false,
			Duration: -1,
			Message:  "error loading newer rows",
			Error:    err,
		}
	}
	rowMap, order := mergeSortRows(rowsOlder, rowsNewer)
	merged, err := table.writeSegment(newer.id, order, rowMap, request.DirPath)
	if err != nil {
		return &SegmentCompactionResponse{
			Ok:       false,
			Duration: -1,
			Message:  "error writing segment",
			Error:    err,
		}
	}
	table.remove(older.id)
	table.remove(newer.id)
	table.insert(merged)
	if err := files.Delete(older.filePath); err != nil {
		// TODO: add error and try later
	}
	if err := files.Delete(newer.filePath); err != nil {
		// TODO: add error and try later
	}
	duration := time.Since(start)
	return &SegmentCompactionResponse{
		Ok:       true,
		Duration: duration,
		Message:  fmt.Sprintf("successfully compacted %d rows in %s", older.rowsNumber+newer.rowsNumber, duration.String()),
		Error:    nil,
	}
}

// TODO: extract encode&save data to method
// Private
func (table *Segments) writeSegment(id int64, order []string, rows *syncmap.SynchronizedMap[string, string], dirPath string) (*segment, error) {
	filePath := fmt.Sprintf(dirPath + "/" + uuid.New().String()) // TODO: handle slash ending
	segment := &segment{
		id:         id,
		rowsNumber: int64(len(order)),
		filePath:   filePath,
		fileSize:   int64(0),
		offset:     METADATA_FIELD_SIZE * 2,
		syncMap:    syncmap.New[string, int64](),
	}
	// Open file for writing segment:
	file, err := files.OpenFileWrite(segment.filePath)
	if err != nil {
		return nil, err
	}
	// Encode and save the segment id:
	encodedId := make([]byte, METADATA_FIELD_SIZE)
	binary.LittleEndian.PutUint64(encodedId, uint64(segment.id))
	bytesWritten, err := file.WriteAt(encodedId, segment.fileSize)
	if err != nil {
		return nil, err
	}
	segment.fileSize += int64(bytesWritten)
	// Encode and save the segment size:
	encodedSize := make([]byte, METADATA_FIELD_SIZE)
	binary.LittleEndian.PutUint64(encodedSize, uint64(segment.id))
	bytesWritten, err = file.WriteAt(encodedSize, segment.fileSize)
	if err != nil {
		return nil, err
	}
	segment.fileSize += int64(bytesWritten)
	// For every row:
	// encode and save the row with it's length:
	for idx, key := range order {
		value, _ := rows.Load(key) // TODO: error handling
		// Encode data:
		encodedRow, err := encodeRow(&persistentRow{key, value})
		if err != nil {
			return nil, err
		}
		// Encode and save the length of the data:
		encodedRowLength := make([]byte, METADATA_FIELD_SIZE)
		binary.LittleEndian.PutUint64(encodedRowLength, uint64(len(encodedRow)))
		encodedRowLengthWriteOffset := segment.fileSize //
		bytesWritten, err = file.WriteAt(encodedRowLength, segment.fileSize)
		if err != nil {
			return nil, err
		}
		segment.fileSize += int64(bytesWritten)
		// Save the encoded data:
		bytesWritten, err = file.WriteAt(encodedRow, segment.fileSize)
		if err != nil {
			return nil, err
		}
		// After saving to file,
		// 	FIRST update segment in memory (save in sparse table) and
		//	SECOND increase the file size (write offset in this context):
		if idx%SEGMENT_SPARSE_TABLE_LVL == 0 {
			segment.syncMap.Store(key, encodedRowLengthWriteOffset)
			segment.index = append(segment.index, key)
		}
		segment.fileSize += int64(bytesWritten)
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
	slices.Sort(segment.index)
	return segment, nil
}

// Private
func (table *Segments) insert(segment *segment) {
	table.mutex.Lock()
	table.segmentsIndex = append(table.segmentsIndex, segment.id)
	slices.Sort(table.segmentsIndex)
	table.segmentsMap.Store(segment.id, segment)
	table.mutex.Unlock()
}

// Private
func (table *Segments) remove(segmentKeyToRemove int64) {
	table.segmentsMap.Delete(segmentKeyToRemove)
	table.mutex.Lock()
	for idx, segmentKey := range table.segmentsIndex {
		if segmentKeyToRemove == int64(segmentKey) {
			table.segmentsIndex = append(table.segmentsIndex[:idx], table.segmentsIndex[idx+1:]...)
			break
		}
	}
	table.mutex.Unlock()
}

// Private
func mergeSortRows(older []*persistentRow, newer []*persistentRow) (*syncmap.SynchronizedMap[string, string], []string) {
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
func (segment *segment) loadRowsFromFile() ([]*persistentRow, error) {
	file, _, err := files.OpenFileRead(segment.filePath)
	if err != nil {
		return nil, err
	}
	offset := segment.offset
	var rows []*persistentRow
	for offset < segment.fileSize {
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
func loadSegmentFromFile(filePath string) (*segment, error) {
	segment := &segment{
		filePath: filePath,
		syncMap:  syncmap.New[string, int64](),
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
		segment.syncMap.Store(row.Key, offset) // TODO: sparse table - don't store everything in memory!
		segment.index = append(segment.index, row.Key)
		offset += int64(bytesRead)
		rowCount++
	}
	slices.Sort(segment.index)
	files.Close(file)
	return segment, nil
}

// Private
func (segment *segment) search(key string) (string, bool, error) {
	// Just open the file, we will be reading from it for sure
	file, fileInfo, err := files.OpenFileRead(segment.filePath)
	if err != nil {
		return "", false, err
	}
	defer files.Close(file)
	var stop, offset int64
	var valueExists bool
	if offset, valueExists = segment.syncMap.Load(key); valueExists {
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
func (segment *segment) findNearestOffset(key string) (int64, int64) {
	var keyStart, keyStop string
	for _, otherKey := range segment.index {
		if key > otherKey {
			keyStart = otherKey
			continue
		}
		if key < otherKey {
			keyStop = otherKey
			break
		}
	}
	start, startExists := segment.syncMap.Load(keyStart)
	if !startExists {
		start = segment.offset
	}
	stop, stopExists := segment.syncMap.Load(keyStop)
	if !stopExists {
		stop = segment.fileSize
	}
	return start, stop
}

// Private
// Assumptions:
//   - file is open and will be closed
//   - offset < fileSize
func readRowFromFile(file *os.File, offset uint64) (*persistentRow, uint64, error) {
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
func encodeRow(segmentRow *persistentRow) ([]byte, error) {
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
func decodeSegment(bytesFromFile []byte) (*persistentRow, error) {
	var bytesBuffer bytes.Buffer
	var row persistentRow
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
