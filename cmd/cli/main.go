package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/c3r/data-storage-engine/memtable"
	"github.com/c3r/data-storage-engine/segment"
)

var memory *memtable.Memtable
var segments segment.SegmentTable
var waitGroup sync.WaitGroup

var MEM int
var DSK int
var TEST_SIZE int = 1000

func main() {
	memory = memtable.New(256)

	for i := 0; i < TEST_SIZE; i++ {
		// fmt.Println(i)
		Save(strconv.Itoa(i), fmt.Sprintf("Wartość numer %d", i))
	}

	time.Sleep(10 * time.Second)

	for i := 0; i < TEST_SIZE; i++ {
		expectedValue := fmt.Sprintf("Wartość numer %d", i)
		actualValue, err, fromMemory := Load(strconv.Itoa(i))
		if err != nil {
			log(err.Error())
		} else if actualValue != expectedValue {
			panic(fmt.Sprintf("actualValue: [%s] != expectedValue [%s]", actualValue, expectedValue))
		}
		if fromMemory {
			MEM++
		} else {
			DSK++
		}
	}
	waitGroup.Wait()
	log(fmt.Sprintf("Values loaded from MEM: %d", MEM))
	log(fmt.Sprintf("Values loaded from DSK: %d", DSK))
}

func Save(key string, value string) error {
	id, memoryFull := memory.Put(key, value)
	if !memoryFull {
		return nil
	}
	waitGroup.Add(1)
	go func(memory *memtable.Memtable, id string) {
		order, table := memory.Dump(id)
		err := segments.Create(id, order, table)
		if err != nil {
			panic(err)
		}
		memory.Clear(id)
		waitGroup.Done()
	}(memory, id)
	return nil
}

func Load(key string) (string, error, bool) {
	fromMemory := true
	value, valueExists := memory.Get(key)
	if !valueExists {
		fromMemory = false
		var err error
		value, err = segments.Load(key)
		if err != nil {
			return "", err, false
		}
	}
	return value, nil, fromMemory
}

func log(format string) {
	now := time.Now().Format(time.StampNano)
	format = now + ": " + format
	fmt.Println(format)
}
