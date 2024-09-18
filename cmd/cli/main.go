package main

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/c3r/data-storage-engine/memtable"
	"github.com/c3r/data-storage-engine/segment"
)

var memory memtable.ToggledMemtable
var segments segment.SegmentTable
var waitGroup sync.WaitGroup

func main() {
	memory.Init()

	for i := 1; i < 1000*10; i++ {
		Save(strconv.Itoa(i), fmt.Sprintf("Wartość numer %d", i))
	}

	time.Sleep(10 * time.Second)

	for i := 1000; i < 1010; i++ {
		expectedValue := fmt.Sprintf("Wartość numer %d", i)
		actualValue, err := Load(strconv.Itoa(i))
		if err != nil {
			log(fmt.Sprintf("Value for key %s not found!", strconv.Itoa(i)))
		} else if actualValue != expectedValue {
			panic(fmt.Sprintf("actualValue: [%s] != expectedValue [%s]", actualValue, expectedValue))
		}
	}

	waitGroup.Wait()
}

func Save(key string, value string) error {
	memory.Put(key, value)
	if memory.IsMaxSize() {
		segmentId := memory.Rotate()
		waitGroup.Add(1)
		go func(memory *memtable.ToggledMemtable) {
			defer waitGroup.Done()
			var seg segment.Segment
			segments.Append(&seg)
			id := segments.Length()
			order, table := memory.Dump(segmentId)
			err := seg.Write(id, order, table)
			if err != nil {
				panic(err)
			}
			memory.Clear(segmentId)
		}(&memory)
	}
	return nil
}

func Load(key string) (string, error) {
	value, valueExists := memory.Get(key)
	if valueExists {
		return value, nil
	}
	for i := segments.Length() - 1; i >= 0; i-- {
		seg := segments.Get(i)
		bytes, valueExists, err := seg.Read(key)
		if err != nil {
			return "", err
		}
		if valueExists {
			row, err := segment.DecodeRow(bytes)
			if err != nil {
				return "", err
			}
			log(fmt.Sprintf("DSK: %s == %s", key, row.Value))
			return row.Value, nil
		}
	}
	e := fmt.Errorf("key %s not found", key)
	return "", errors.New(e.Error())
}

func log(format string) {
	now := time.Now().Format(time.StampNano)
	format = now + ": " + format
	fmt.Println(format)
}
