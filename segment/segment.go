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

	"github.com/c3r/data-storage-engine/files"
	"github.com/c3r/data-storage-engine/syncmap"
	"github.com/google/uuid"
)

const FILE_PATH_TEMPLATE = "/tmp/tb_storage_%s"
const DATA_LENGHT_BYTES_SIZE = 8
const SEGMENT_SPARSE_TABLE_LVL = 5

type Segment struct {
	FilePath string
	SyncMap  *syncmap.SynchronizedMap[string, int64]
	Index    []string
}

type segmentRow struct {
	Key   string
	Value string
}

type SegmentTable struct {
	segments []*Segment
}

func (table *SegmentTable) CreatePersistentSegment(order []string, rows *syncmap.SynchronizedMap[string, string], segmentsFilePath string) (string, error) {
	filePath := fmt.Sprintf(segmentsFilePath + "/" + uuid.New().String())
	segment := &Segment{
		FilePath: filePath,
		SyncMap:  syncmap.New[string, int64](),
	}
	fileWriteOffset := int64(0)
	// Open file for writing segment
	file, err := files.OpenFileWrite(segment.FilePath)
	if err != nil {
		return filePath, err
	}
	for idx, key := range order {
		value, _ := rows.Load(key)
		// Encode data
		encodedSegment, err := encodeSegment(&segmentRow{key, value})
		if err != nil {
			return filePath, err
		}
		// First encode and save the length of the data
		encodedDataLength := make([]byte, DATA_LENGHT_BYTES_SIZE)
		binary.LittleEndian.PutUint64(encodedDataLength, uint64(len(encodedSegment)))
		bytesWritten, err := file.WriteAt(encodedDataLength, fileWriteOffset)
		if err != nil {
			return filePath, err
		}
		// Save the encoded data
		bytesWritten, err = file.WriteAt(encodedSegment, fileWriteOffset+int64(bytesWritten))
		if err != nil {
			return filePath, err
		}
		// After saving to file, update segment in memory (save in sparse table)
		if idx%SEGMENT_SPARSE_TABLE_LVL == 0 {
			segment.SyncMap.Store(key, fileWriteOffset)
			segment.Index = append(segment.Index, key)
		}
		fileWriteOffset += int64(bytesWritten) + DATA_LENGHT_BYTES_SIZE
	}
	// Sync and close file
	err = file.Sync()
	if err != nil {
		return filePath, err
	}
	err = file.Close()
	if err != nil {
		return filePath, err
	}
	slices.Sort(segment.Index)
	table.Insert(segment)
	return filePath, nil
}

func (table *SegmentTable) Insert(segment *Segment) {
	// TODO: need to number segments - race condition when threads are persisting segments
	table.segments = append(table.segments, segment)
}

func (table *SegmentTable) Load(key string) (string, error) {
	// TODO: Iterating from the newest to oldest: the newest value is saved later
	for _, segment := range table.segments {
		value, valueExists, err := segment.search(key)
		if err != nil {
			return "", err
		}
		if valueExists {
			return value, nil
		}
	}
	e := fmt.Errorf("key %s not found", key)
	return "", errors.New(e.Error())
}

func (table *SegmentTable) LoadSegmentsFromDisk(segmentFilesDirectory string) error {
	directoryEntries, err := os.ReadDir(segmentFilesDirectory)
	if err != nil {
		return err
	}
	for _, directoryEntry := range directoryEntries {
		if !directoryEntry.Type().IsRegular() {
			continue
		}
		filePath := fmt.Sprintf("%s/%s", segmentFilesDirectory, directoryEntry.Name())
		seg := loadSegmentFromFile(filePath)
		table.Insert(seg)
	}
	return nil
}

func (table *SegmentTable) Compact(segmentsDirPath string) {
	logger := log.Default()
	segmentsNum := len(table.segments)
	if segmentsNum < 2 {
		return
	}

	olderFilePath := table.segments[segmentsNum-2].FilePath
	newerFilePath := table.segments[segmentsNum-1].FilePath

	older := loadRowsFromFile(olderFilePath)
	newer := loadRowsFromFile(newerFilePath)
	result, order := mergeSortRows(older, newer)
	if filePath, err := table.CreatePersistentSegment(order, result, segmentsDirPath); err == nil {
		table.segments = remove(table.segments, segmentsNum-1)
		table.segments = remove(table.segments, segmentsNum-1)
		logger.Printf("File %s saved.", filePath)
		if err := files.Delete(olderFilePath); err != nil {
			panic(err)
		}
		logger.Printf("File %s deleted.", olderFilePath)
		if err := files.Delete(newerFilePath); err != nil {
			panic(err)
		}
		logger.Printf("File %s deleted.", newerFilePath)
	} else {
		panic(err)
	}
}

func remove(slice []*Segment, s int) []*Segment {
	return append(slice[:s], slice[s+1:]...)
}

func mergeSortRows(older []*segmentRow, newer []*segmentRow) (*syncmap.SynchronizedMap[string, string], []string) {
	result := syncmap.New[string, string]()
	var index []string
	iOlder := 0
	iNewer := 0
	for iNewer <= len(older)-1 && iOlder <= len(newer)-1 {
		if older[iOlder].Key == newer[iNewer].Key {
			result.Store(newer[iNewer].Key, newer[iNewer].Value)
			index = append(index, newer[iNewer].Key)
			iNewer++
			iOlder++
		} else if older[iOlder].Key < newer[iNewer].Key {
			result.Store(older[iOlder].Key, older[iOlder].Value)
			index = append(index, older[iOlder].Key)
			iOlder++
		} else {
			result.Store(newer[iNewer].Key, newer[iNewer].Value)
			index = append(index, newer[iNewer].Key)
			iNewer++
		}
		if iOlder == len(older) {
			for _, row := range newer[iNewer:] {
				result.Store(row.Key, row.Value)
				index = append(index, row.Key)
			}
		} else if iNewer == len(newer) {
			for _, row := range older[iOlder:] {
				result.Store(row.Key, row.Value)
				index = append(index, row.Key)
			}
		}
	}

	result.Range(func(key, value string) bool {
		fmt.Printf("%s = %s\n", key, value)
		return true
	})

	return result, index
}

func loadRowsFromFile(filePath string) []*segmentRow {
	file, fileInfo, err := files.OpenFileRead(filePath)
	if err != nil {
		panic(err)
	}
	offset := uint64(0)
	var rows []*segmentRow
	for offset < uint64(fileInfo.Size()) {
		row, bytesRead, err := readRowFromFile(file, uint64(offset))
		if err != nil {
			panic(err)
		}
		rows = append(rows, row)
		offset += bytesRead
	}
	return rows
}

// Private
func loadSegmentFromFile(filePath string) *Segment {
	seg := &Segment{
		FilePath: filePath,
		SyncMap:  syncmap.New[string, int64](),
	}
	file, fileInfo, err := files.OpenFileRead(filePath)
	if err != nil {
		panic(err)
	}
	offset := int64(0)
	rowCount := 0
	for offset < fileInfo.Size() {
		row, bytesRead, err := readRowFromFile(file, uint64(offset))
		if err != nil {
			panic(err)
		}
		seg.SyncMap.Store(row.Key, offset)
		seg.Index = append(seg.Index, row.Key)
		offset += int64(bytesRead)
		rowCount++
	}
	slices.Sort(seg.Index)
	files.Close(file)
	return seg
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
		offset, stop = segment.findNearestOffset(key, fileInfo.Size())
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
func (segment *Segment) findNearestOffset(key string, offsetUpperBound int64) (int64, int64) {
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
		start = int64(0)
	}
	stop, stopExists := segment.SyncMap.Load(keyStop)
	if !stopExists {
		stop = offsetUpperBound
	}
	return start, stop
}

// Private
// Assumptions:
//   - file is open and will be closed
//   - offset < fileSize
func readRowFromFile(file *os.File, offset uint64) (*segmentRow, uint64, error) {
	dataLenBytes, err := files.Read(file, DATA_LENGHT_BYTES_SIZE, offset)
	if err != nil {
		return nil, 0, err
	}
	dataLen := binary.LittleEndian.Uint64(dataLenBytes)
	bytes, err := files.Read(file, dataLen, offset+DATA_LENGHT_BYTES_SIZE)
	if err != nil {
		return nil, 0, err
	}
	row, err := decodeSegment(bytes)
	if err != nil {
		return nil, 0, err
	}
	return row, dataLen + DATA_LENGHT_BYTES_SIZE, nil
}

// Private
func encodeSegment(segmentRow *segmentRow) ([]byte, error) {
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
