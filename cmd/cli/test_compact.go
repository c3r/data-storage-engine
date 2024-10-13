package main

import (
	"log"
	"time"

	"github.com/c3r/data-storage-engine/storage"
)

var logger = log.Default()

var maxSegmentsSize = 10
var maxSegments = 1024
var segmentThreads = 5
var segmentsDir = "/tmp/tb_storage"

var stor = storage.New(int64(maxSegmentsSize), int64(maxSegments), segmentThreads, segmentsDir)

func main() {

	// Segment 1:
	stor.Save("a", "pierwszy")
	stor.Save("b", "pierwszy")
	stor.Save("c", "pierwszy")
	stor.Save("d", "pierwszy")
	stor.Save("e", "pierwszy")
	stor.Save("f", "pierwszy")
	stor.Save("g", "pierwszy")
	stor.Save("h", "pierwszy")
	stor.Save("i", "pierwszy")
	stor.Save("k", "pierwszy")
	// Segment 2:
	stor.Save("ax", "drugi")
	stor.Save("bx", "drugi")
	stor.Save("cx", "drugi")
	stor.Save("d", "drugi")
	stor.Save("ex", "drugi")
	stor.Save("f", "drugi")
	stor.Save("gx", "drugi")
	stor.Save("ha", "drugi")
	stor.Save("ix", "drugi")
	stor.Save("jx", "drugi")
	// Segment 3:
	stor.Save("axx", "trzeci")
	stor.Save("bxx", "trzeci")
	stor.Save("cxx", "trzeci")
	stor.Save("dxx", "trzeci")
	stor.Save("exx", "trzeci")
	stor.Save("fxx", "trzeci")
	stor.Save("gxx", "trzeci")
	stor.Save("h", "trzeci")
	stor.Save("ixx", "trzeci")
	stor.Save("jxx", "trzeci")
	// Rest:
	stor.Save("ZZZZ", "20")
	stor.Save("ZZZZ", "21")
	stor.Save("ZZZZ", "22")
	stor.Save("ZZZZ", "23")

	sleepFor := time.Duration(100) * time.Millisecond
	time.Sleep(sleepFor)

	stor.Compact(segmentsDir)

	assert("f", "drugi")
	assert("ha", "drugi")
	assert("d", "drugi")
	assert("g", "pierwszy")
	assert("k", "pierwszy")
	assert("jx", "drugi")
	assert("jxx", "trzeci")
	assert("h", "trzeci")
	assert("ZZZZ", "23")
}

func assert(key string, expectedValue string) {
	value, _, _ := stor.Load(key)
	if value == expectedValue {
		logger.Println("OK!")
	} else {
		logger.Printf("[%s]: expected: \"%s\", actual: \"%s\"", key, expectedValue, value)
	}

}
