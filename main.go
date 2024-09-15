package main

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
)

var index_map_key_order = []string{}
var index_map = map[string]string{}
var persisted_segments_num = 0

const MAP_REFERENCE_MAX_SIZE = 10
const TEST_SIZE = 100

// var cmd = "write"

type SortedSegment struct {
	size  int
	key   string
	value string
}

func main() {

	// rand.Seed(time.Now().UnixNano())

	// TODO: here - check how many persisted segments there are on disk at start
	// persisted_segments_num = ???

	// args := os.Args[1:]
	// cmd := args[0]

	var saved_keys []string

	for i := 0; i < TEST_SIZE; i++ {
		saved_key := uuid.New().String()
		RunCommand("write", saved_key, uuid.New().String())
		saved_keys = append(saved_keys, saved_key)
	}

	for i := 0; i < TEST_SIZE; i++ {
		saved_key := saved_keys[i]
		RunCommand("read", saved_key, "")
	}

	// RunCommand("write", "a", "value_for_a")
	// RunCommand("write", "b", "value_for_b")
	// RunCommand("write", "c", "value_for_c")
	// RunCommand("write", "d", "value_for_d")
	// RunCommand("read", "a", "")
	// RunCommand("read", "b", "")
	// RunCommand("read", "c", "")
	// RunCommand("read", "d", "")
}

func RunCommand(cmd string, key string, value string) {
	switch cmd {
	case "write":
		Write(key, value)
		Persist()
	case "read":
		ret_value, value_exists := Read(key)
		if value_exists {
			Debug("Value for key [%s] = [%s]", key, ret_value)
		} else {
			Debug("No value for key [%s]", key, "")
		}
	}
}

func Debug(format string, val1 string, val2 string) {
	now := time.Now().Format(time.StampNano)
	format = "[DEBUG] " + now + ": " + format + "\n"

	if val1 == "" && val2 == "" {
		fmt.Printf(format)
	} else if val2 == "" {
		fmt.Printf(format, val1)
	} else {
		fmt.Printf(format, val1, val2)
	}
}

func Write(key string, value string) {
	index_map[key] = value
	index_map_key_order = append(index_map_key_order, key)
	// Debug("Writing value [%s] for key [%s] done.", value, key)
	if !slices.IsSorted(index_map_key_order) {
		Debug("Sorting index...", "", "")
		slices.Sort(index_map_key_order)
		Debug("Sorting index done.", "", "")
	}
}

func Persist() {
	if len(index_map) < MAP_REFERENCE_MAX_SIZE {
		return
	}
	Debug("MAP_REFERENCE_MAX_SIZE passed. Persisting index map to a new segment file...", "", "")

	file, err := os.Create("/tmp/dat2")
	if err != nil {
		panic(err)
	}

	for key, value := range index_map {
		var segment = new(SortedSegment)
		// segment.size = ???
		segment.key = key
		segment.value = value
		file.Write()
	}

	file.Sync()
	file.Close()

	// When the memtable gets bigger than some threshold—typically a few megabytes—write it out to disk as an SSTable file.
	// This can be done efficiently because the tree already maintains the key-value pairs sorted by key.
	// The new SSTable file becomes the most recent segment of the database.
	// !!!: While the SSTable is being written out to disk, writes can continue to a new memtable instance.
	Debug("Persisting index map to a new segment file done.", "", "")
}

func ToGOB64(m SortedSegment) string {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(m)
	if err != nil {
		fmt.Println(`failed gob Encode`, err)
	}
	return base64.StdEncoding.EncodeToString(b.Bytes())
}

// go binary decoder
func FromGOB64(str string) SortedSegment {
	m := SortedSegment{}
	by, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		fmt.Println(`failed base64 Decode`, err)
	}
	b := bytes.Buffer{}
	b.Write(by)
	d := gob.NewDecoder(&b)
	err = d.Decode(&m)
	if err != nil {
		fmt.Println(`failed gob Decode`, err)
	}
	return m
}

func Read(key string) (string, bool) {
	// In order to serve a read request, first try to find the key in the memtable,
	Debug("Searching for value for key [%s]...", key, "")
	ret_value, value_exists := index_map[key]
	if value_exists {
		Debug("Value for key [%s] found in memtable, returning.", key, "")
		return ret_value, true
	}
	Debug("Value for key [%s] not found in memtable, searching in persisted segments...", key, "")
	// then in the persisted segment files
	for segment_idx := 0; segment_idx < persisted_segments_num; segment_idx++ {
		Debug("Searching for key [%s] in persisted segment [%s]...", key, strconv.Itoa(segment_idx))
		ret_value, value_exists = ReadSegment(segment_idx, key)
		if value_exists {
			Debug("Value for key [%s] found in segment [%s], returning.", key, strconv.Itoa(segment_idx))
			return ret_value, true
		}
	}
	Debug("Value for key [%s] not found in persisted segments, returning empty value.", key, "")
	return "", false
}

func ReadSegment(segment_idx int, key string) (string, bool) {
	Debug("Reading from segment [%s]...", strconv.Itoa(segment_idx), "")
	return "from_read_segment", true
}

func Delete() {
}

func Compact() {
	// From time to time, run a merging and compaction process in the background
	// to combine segment files and to discard overwritten or deleted values.
}
