// package main

// import (
// 	"fmt"
// 	"log"
// 	"math"
// 	"math/rand/v2"
// 	"strconv"
// 	"sync"
// 	"sync/atomic"
// 	"time"

// 	"github.com/c3r/data-storage-engine/storage"
// )

// var saveRequestCount atomic.Uint64
// var readMemRequestCount atomic.Uint64
// var readDskRequestCount atomic.Uint64
// var saveDuration atomic.Uint64
// var readMemDuration atomic.Uint64
// var readDskDuration atomic.Uint64

// func remove(slice []string, s int) []string {
// 	return append(slice[:s], slice[s+1:]...)
// }

// func client(id string, storage *storage.Storage, size int32, wg *sync.WaitGroup) {
// 	defer wg.Done()
// 	logger := log.Default()
// 	toSave := []string{}
// 	savedIdx := []string{}

// 	// Generate data

// 	length := 0
// 	for length <= 0 {
// 		length = int(math.Pow(10, float64(size)) * rand.ExpFloat64() / 2)
// 	}

// 	for i := 0; i < length; i++ {
// 		key := fmt.Sprintf("%s:%s", id, strconv.Itoa(i))
// 		toSave = append(toSave, key)
// 	}

// 	clientStart := time.Now()
// 	for {
// 		if time.Since(clientStart).Minutes() > 1 {
// 			break
// 		}
// 		// Sleep for a random time
// 		randomDuration := int32(10 + rand.Float64()*math.Pow(10, float64(6-size)))
// 		sleepFor := time.Duration(randomDuration) * time.Millisecond
// 		time.Sleep(sleepFor)
// 		if rand.Int()%2 == 0 {
// 			if len(toSave) == 0 {
// 				continue
// 			}
// 			idx := rand.IntN(len(toSave))
// 			key := toSave[idx]
// 			toSave = remove(toSave, idx)
// 			value := fmt.Sprintf("[%s]-%s", id, key)
// 			start := time.Now()
// 			storage.Save(key, value)
// 			saveRequestCount.Add(1)
// 			saveDuration.Add(uint64(time.Since(start).Microseconds()))
// 			savedIdx = append(savedIdx, key)
// 		} else {
// 			// If there are no saved keys, decide again if save or load
// 			if len(savedIdx) == 0 {
// 				continue
// 			}
// 			idx := rand.IntN(len(savedIdx))
// 			key := savedIdx[idx]
// 			start := time.Now()
// 			actualValue, err, isMem := storage.Load(key)
// 			if isMem {
// 				readMemRequestCount.Add(1)
// 				readMemDuration.Add(uint64(time.Since(start).Microseconds()))
// 			} else {
// 				readDskRequestCount.Add(1)
// 				readDskDuration.Add(uint64(time.Since(start).Microseconds()))
// 			}

// 			if err != nil {
// 				logger.Println(err)
// 			}
// 			expectedValue := fmt.Sprintf("[%s]-%s", id, key)
// 			if expectedValue != actualValue {
// 				logger.Fatalf("ASSERTION ERROR:\nexpected value = %s\nactualvalue = %s\n\n", expectedValue, actualValue)
// 			}
// 		}
// 	}
// }

// func main() {
// 	logger := log.Default()
// 	maxSegmentsSize := 1024 * 10
// 	maxSegments := 1024
// 	segmentThreads := 5
// 	clients := 500
// 	segmentsDir := "/tmp/tb_storage"

// 	storage := storage.New(int64(maxSegmentsSize), int64(maxSegments), segmentThreads, segmentsDir)
// 	wg := &sync.WaitGroup{}
// 	start := time.Now()
// 	for i := 0; i < clients; i++ {
// 		id := strconv.Itoa(i)
// 		wg.Add(1)
// 		size := 2 + rand.Int32N(5)
// 		go client(id, storage, size, wg)
// 	}
// 	wg.Wait()
// 	elapsed := time.Since(start)
// 	logger.Printf("*** saved: %d", saveRequestCount.Load())
// 	logger.Printf("*** loaded from mem: %d", readMemRequestCount.Load())
// 	logger.Printf("*** loaded from dsk: %d", readDskRequestCount.Load())
// 	logger.Printf("*** mean save: %d us", saveDuration.Load()/saveRequestCount.Load())
// 	logger.Printf("*** mean load mem: %d us", readMemDuration.Load()/readMemRequestCount.Load())
// 	if readDskRequestCount.Load() > 0 {
// 		logger.Printf("*** mean load dsk: %d us", readDskDuration.Load()/readDskRequestCount.Load())
// 	}
// 	logger.Printf("*** total: %d req/s", (readMemRequestCount.Load()+readDskRequestCount.Load()+saveRequestCount.Load())/uint64(elapsed.Seconds()))
// }
