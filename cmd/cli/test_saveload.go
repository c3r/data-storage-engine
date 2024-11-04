package main

import (
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c3r/data-storage-engine/storage"
)

var saveRequestCount atomic.Uint64
var readMemRequestCount atomic.Uint64
var readDskRequestCount atomic.Uint64
var saveDuration atomic.Uint64
var readMemDuration atomic.Uint64
var readDskDuration atomic.Uint64

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.IntN(len(letterBytes))]
	}
	return string(b)
}

func remove(slice []string, s int) []string {
	return append(slice[:s], slice[s+1:]...)
}

func client(clientId string, storage *storage.Storage, size int32, wg *sync.WaitGroup, minutes int) {
	defer wg.Done()
	logger := log.Default()

	valuesToSave := map[string]string{}
	keysToSave := []string{}

	savedKeys := []string{}
	expectedValues := map[string]string{}

	// Generate data

	length := 0
	for length <= 0 {
		length = int(math.Pow(10, float64(size)) * rand.ExpFloat64() / 2)
	}

	for i := 0; i < length; i++ {
		key := fmt.Sprintf("%s:%s", clientId, strconv.Itoa(i))
		valuesToSave[key] = RandStringBytes(256)
		keysToSave = append(keysToSave, key)
	}

	clientStart := time.Now()
	for {
		if time.Since(clientStart).Minutes() > float64(minutes) {
			break
		}
		// Sleep for a random time
		randomDuration := int32(10 + rand.Float64()*math.Pow(10, float64(6-size)))
		sleepFor := time.Duration(randomDuration) * time.Millisecond
		time.Sleep(sleepFor)
		if rand.Int()%2 == 0 {
			if len(valuesToSave) == 0 {
				continue
			}
			idx := rand.IntN(len(valuesToSave))
			key := keysToSave[idx]
			value := fmt.Sprintf("[%s]-%s:::%s", clientId, key, valuesToSave[key])
			expectedValues[key] = valuesToSave[key]
			delete(valuesToSave, key)
			remove(keysToSave, idx)
			start := time.Now()
			storage.Save(key, value)
			saveRequestCount.Add(1)
			saveDuration.Add(uint64(time.Since(start).Microseconds()))
			savedKeys = append(savedKeys, key)
		} else {
			// If there are no saved keys, decide again if save or load
			if len(savedKeys) == 0 {
				continue
			}
			idx := rand.IntN(len(savedKeys))
			key := savedKeys[idx]
			start := time.Now()
			actualValue, err, isMem := storage.Load(key)
			if isMem {
				readMemRequestCount.Add(1)
				readMemDuration.Add(uint64(time.Since(start).Microseconds()))
			} else {
				readDskRequestCount.Add(1)
				readDskDuration.Add(uint64(time.Since(start).Microseconds()))
			}

			if err != nil {
				logger.Println(err)
			}
			expectedValue := fmt.Sprintf("[%s]-%s:::%s", clientId, key, expectedValues[key])
			if expectedValue != actualValue {
				logger.Fatalf("ASSERTION ERROR:\nexpected value = %s\nactualvalue = %s\n\n", expectedValue, actualValue)
			}
		}
	}
}

func main() {
	logger := log.Default()
	maxSegmentsSize := 1024 * 20
	maxSegments := 1024
	segmentThreads := 2
	clients := 200
	segmentsDir := "/tmp/tb_storage"
	minutes := 5

	storage := storage.NewStorage(int64(maxSegmentsSize), int64(maxSegments), segmentThreads, segmentsDir, 5)
	wg := &sync.WaitGroup{}
	start := time.Now()
	for i := 0; i < clients; i++ {
		id := strconv.Itoa(i)
		wg.Add(1)
		size := 2 + rand.Int32N(5)
		go client(id, storage, size, wg, minutes)
	}
	wg.Wait()
	elapsed := time.Since(start)
	logger.Printf("*** saved: %d", saveRequestCount.Load())
	logger.Printf("*** loaded from mem: %d", readMemRequestCount.Load())
	logger.Printf("*** loaded from dsk: %d", readDskRequestCount.Load())
	logger.Printf("*** mean save: %d us", saveDuration.Load()/saveRequestCount.Load())
	logger.Printf("*** mean load mem: %d us", readMemDuration.Load()/readMemRequestCount.Load())
	if readDskRequestCount.Load() > 0 {
		logger.Printf("*** mean load dsk: %d us", readDskDuration.Load()/readDskRequestCount.Load())
	}
	logger.Printf("*** total: %d req/s", (readMemRequestCount.Load()+readDskRequestCount.Load()+saveRequestCount.Load())/uint64(elapsed.Seconds()))
}
