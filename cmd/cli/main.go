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
var loadRequestCount atomic.Uint64
var meanSavedGlobal atomic.Uint64
var meanLoadedMemGlobal atomic.Uint64
var meanLoadedDskGlobal atomic.Uint64

func remove(slice []string, s int) []string {
	return append(slice[:s], slice[s+1:]...)
}

func client(id string, storage *storage.Storage, size int32, wg *sync.WaitGroup) {
	defer wg.Done()
	logger := log.Default()
	toSave := []string{}
	saved := map[string]time.Duration{}
	savedIdx := []string{}
	loadedMem := map[string]time.Duration{}
	loadedDsk := map[string]time.Duration{}

	// Generate data

	length := 0
	for length <= 0 {
		length = int(math.Pow(10, float64(size)) * rand.ExpFloat64() / 2)
	}

	for i := 0; i < length; i++ {
		key := fmt.Sprintf("%s:%s", id, strconv.Itoa(i))
		toSave = append(toSave, key)
	}

	clientStart := time.Now()
	// logger.Printf("C%-4s (%1d)[%-8d]", id, size, length)
	for {
		// if len(toSave) == 0 && (len(loadedDsk)+len(loadedMem)) == length {
		if time.Since(clientStart).Minutes() > 1 {
			var meanSaved int64
			var meanLoadedMem int64
			var meanLoadedDsk int64
			for key, val := range saved {
				meanSaved += val.Microseconds()
				meanSavedGlobal.Add(uint64(val.Microseconds()))
				if duration, valueExists := loadedDsk[key]; valueExists {
					meanLoadedDsk += duration.Microseconds()
					meanLoadedDskGlobal.Add(uint64(duration.Microseconds()))
				}
				if duration, valueExists := loadedMem[key]; valueExists {
					meanLoadedMem += duration.Microseconds()
					meanLoadedMemGlobal.Add(uint64(duration.Microseconds()))
				}
			}

			if len(saved) > 0 {
				meanSaved /= int64(len(saved))
			}
			if len(loadedMem) > 0 {
				meanLoadedMem /= int64(len(loadedMem))
			}
			if len(loadedDsk) > 0 {
				meanLoadedDsk /= int64(len(loadedDsk))
			}
			// logger.Printf("C%-4s (%1d)[%-8d] %8d %-8d %8d %-8d %8d %-8d", id, size, length, len(saved), meanSaved, len(loadedMem), meanLoadedMem, len(loadedDsk), meanLoadedDsk)
			break
		}
		// Sleep for a random time
		sleep := int32(10 + rand.Float64()*math.Pow(10, float64(6-size)))
		// logger.Printf("sleeping for %d (size: %d)", sleep, size)
		time.Sleep(time.Duration(sleep) * time.Millisecond)
		doSave := rand.Int()%2 == 0
		if doSave {
			if len(toSave) == 0 {
				continue
			}
			saveRequestCount.Add(1)
			idx := rand.IntN(len(toSave))
			key := toSave[idx]
			toSave = remove(toSave, idx)
			// if len(toSave) == 0 {
			// 	logger.Printf("Client %s (size: %d) (data: %d) saved all his keys", id, size, length)
			// }
			value := fmt.Sprintf("[%s]-%s", id, key)
			start := time.Now()
			storage.Save(key, value)
			saved[key] = time.Since(start)
			savedIdx = append(savedIdx, key)
		} else {
			// If there are no saved keys, decide again if save or load
			if len(saved) == 0 {
				continue
			}
			loadRequestCount.Add(1)
			idx := rand.IntN(len(savedIdx))
			key := savedIdx[idx]
			start := time.Now()
			actualValue, err, isMem := storage.Load(key)
			if isMem {
				loadedMem[key] = time.Since(start)
			} else {
				loadedDsk[key] = time.Since(start)
			}

			if err != nil {
				logger.Println(err)
			}
			expectedValue := fmt.Sprintf("[%s]-%s", id, key)
			if expectedValue != actualValue {
				logger.Fatalf("ASSERTION ERROR:\nexpected value = %s\nactualvalue = %s\n\n", expectedValue, actualValue)
			}
		}
	}
}

func main() {
	logger := log.Default()
	maxSegmentsSize := 1024
	maxSegments := 1024
	segmentThreads := 5
	clients := 1000

	storage := storage.New(int64(maxSegmentsSize), int64(maxSegments), segmentThreads)
	wg := &sync.WaitGroup{}
	start := time.Now()
	for i := 0; i < clients; i++ {
		id := strconv.Itoa(i)
		wg.Add(1)
		size := 2 + rand.Int32N(5)
		go client(id, storage, size, wg)
	}
	wg.Wait()
	elapsed := time.Since(start)
	logger.Printf("*** save: %d req/s", saveRequestCount.Load()/uint64(elapsed.Seconds()))
	logger.Printf("*** load: %d req/s", loadRequestCount.Load()/uint64(elapsed.Seconds()))
	logger.Printf("*** mean save: %d us", meanSavedGlobal.Load()/saveRequestCount.Load())
	logger.Printf("*** mean load mem: %d us", meanLoadedMemGlobal.Load()/saveRequestCount.Load())
	logger.Printf("*** mean load dsk: %d us", meanLoadedDskGlobal.Load()/saveRequestCount.Load())
	logger.Printf("*** total: %d req/s", (loadRequestCount.Load()+saveRequestCount.Load())/uint64(elapsed.Seconds()))
}
