package storage

import (
	"errors"
	"fmt"
	"log"
	"os"
	"slices"

	"github.com/c3r/data-storage-engine/files"
	"github.com/c3r/data-storage-engine/memtable"
	"github.com/c3r/data-storage-engine/segment"
	"github.com/c3r/data-storage-engine/syncmap"
)

var logger log.Logger = *log.Default()

const DIR = "/tmp/tb_storage"

type Storage struct {
	memory   *memtable.MemtableManager
	segments segment.SegmentTable
}

func New(maxSegmentSize int64, maxSegments int64, segmentThreads int) *Storage {
	memory := memtable.New(maxSegmentSize, maxSegments)
	storage := &Storage{memory, segment.SegmentTable{}}
	storage.initSegmentThreads(segmentThreads)
	storage.loadSegmentsFromDisk()
	return storage
}

func (storage *Storage) loadSegmentsFromDisk() {
	logger.Printf("Loading data from files in %s...", DIR)
	entries, err := os.ReadDir(DIR)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			logger.Printf("Directory %s does not exist. No files to load.", DIR)
			return
		}
		panic(err)
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		filePath := fmt.Sprintf("%s/%s", DIR, entry.Name())
		seg := &segment.Segment{
			FilePath: filePath,
			SyncMap:  syncmap.New[string, int64](),
		}
		file, fileInfo, err := files.OpenFileRead(filePath)
		// logger.Printf("Opened file %s for reading segment...", filePath)
		if err != nil {
			panic(err)
		}
		offset := int64(0)
		rowCount := 0
		for offset < fileInfo.Size() {
			row, bytesRead, err := segment.ReadFromFile(file, uint64(offset))
			if err != nil {
				panic(err)
			}
			// logger.Printf("Read: [key:%s val:%s offset:%d size:%d]", row.Key, row.Value, offset, bytesRead)
			seg.SyncMap.Store(row.Key, offset)
			seg.Index = append(seg.Index, row.Key)
			offset += int64(bytesRead)
			rowCount++
		}
		slices.Sort(seg.Index)
		storage.segments.Insert(seg)
		logger.Printf("%s | %10dB | %5d entries", filePath, fileInfo.Size(), rowCount)
		files.Close(file)
	}
	logger.Println("All segments loaded successfully!")
	logger.Println("---")
}

func (storage *Storage) initSegmentThreads(num int) {
	for i := 0; i < num; i++ {
		go storage.segmentThread(i)
	}
}

func (storage *Storage) segmentThread(id int) {
	logger.Printf("Initializing segment thread %d", id)
	for {
		id, rows := storage.memory.Dump()
		order := []string{}
		rows.Range(func(key, value string) bool {
			order = append(order, key)
			return true
		})
		slices.Sort(order)
		err := storage.segments.Create(order, rows)
		if err != nil {
			panic(err)
		}
		storage.memory.Clear(id)
	}
}

func (storage *Storage) Save(key string, value string) {
	storage.memory.Store(key, value)
}

func (storage *Storage) Load(key string) (string, error, bool) {
	fromMemory := true
	value, valueExists := storage.memory.Load(key)
	if !valueExists {
		fromMemory = false
		var err error
		value, err = storage.segments.Load(key)
		if err != nil {
			return "", err, false
		}
	}
	return value, nil, fromMemory
}
