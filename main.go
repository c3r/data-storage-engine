package main

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/tjarratt/babble"
)

var index_map_key_order = []string{}
var index_map = map[string]IndexItem{}
var persisted_segments_num = 0
var write_offset uint64 = 0

const MAP_REFERENCE_MAX_SIZE = 100
const TEST_SIZE = 1000
const FILE_PATH = "/tmp/dat2"

// var cmd = "write"

type Segment struct {
	Size  uint64
	Key   string
	Value string
}

type IndexItem struct {
	Offset uint64
	Size   uint64
}

func main() {

	// TODO: here - check how many persisted segments there are on disk at start
	// persisted_segments_num = ???

	// args := os.Args[1:]
	// cmd := args[0]

	var saved_keys []string

	// save data
	babbler := babble.NewBabbler()
	for i := 0; i < TEST_SIZE; i++ {
		saved_key := uuid.New().String()
		RunCommand("write", saved_key, babbler.Babble())
		saved_keys = append(saved_keys, saved_key)
	}

	// read data
	for i := TEST_SIZE - 1; i > 0; i-- {
		saved_key := saved_keys[i]
		RunCommand("read", saved_key, "")
	}
}

func RunCommand(cmd string, key string, value string) {
	switch cmd {
	case "write":

		// write value to disk
		Debug2("Writing to disk: [%s] | [%s]", key, value)
		offset, bytes_written := Write(key, value)

		// update index map with index key and offset on disk
		var index_item IndexItem
		index_item.Offset = offset
		index_item.Size = bytes_written
		index_map[key] = index_item
		index_map_key_order = append(index_map_key_order, key)
		write_offset = write_offset + bytes_written

		// if the key order is not sorted, sort the order
		if !slices.IsSorted(index_map_key_order) {
			slices.Sort(index_map_key_order)
		}

		if len(index_map) > MAP_REFERENCE_MAX_SIZE {
			Debug0("MAP_REFERENCE_MAX_SIZE passed. Persisting index map to a new segment file...")
			PersistIndexMap()
		}

	case "read":
		segment, value_exists := Read(key)
		if value_exists {
			Debug2("Read from disk: [%s] | [%s]", segment.Key, segment.Value)
		} else {
			msg := fmt.Sprintf("No segment for key [%s] found!", key)
			panic(msg)
		}
	}
}

func Debug0(format string) {
	Debug(format, "", "", "")
}

func Debug1(format string, value string) {
	Debug(format, value, "", "")
}

func Debug2(format string, value1 string, value2 string) {
	Debug(format, value1, value2, "")
}

func Debug3(format string, value1 string, value2 string, value3 string) {
	Debug(format, value1, value2, value3)
}

func Debug(format string, value1 string, value2 string, value3 string) {
	now := time.Now().Format(time.StampNano)
	format = "[DEBUG] " + now + ": " + format + "\n"

	if value1 == "" && value2 == "" && value3 == "" {
		fmt.Print(format)
		return
	}

	if value1 != "" && value2 == "" && value3 == "" {
		fmt.Printf(format, value1)
		return
	}

	if value1 != "" && value2 != "" && value3 == "" {
		fmt.Printf(format, value1, value2)
		return
	}

	if value1 != "" && value2 != "" && value3 != "" {
		fmt.Printf(format, value1, value2, value3)
		return
	}

}

func Write(key string, value string) (uint64, uint64) {
	// ------ Open file from disk to write:
	var file *os.File
	if _, err := os.Stat(FILE_PATH); err == nil {
		file, err = os.OpenFile(FILE_PATH, os.O_WRONLY, 0666)
		if err != nil {
			panic(err)
		}
	} else if errors.Is(err, os.ErrNotExist) {
		Debug1("File [%s] does not exist, creating...", FILE_PATH)
		file, err = os.Create(FILE_PATH)
		if err != nil {
			panic(err)
		}
	} else {
		panic(err)
	}

	// ------ Create data segment:
	segment := new(Segment)
	segment.Key = key
	segment.Value = value

	// ------ Encode:
	bytes_buffer := bytes.Buffer{}
	gob_encoder := gob.NewEncoder(&bytes_buffer)
	err := gob_encoder.Encode(segment)
	if err != nil {
		Debug1("Error: %s", err.Error())
		Debug1("Failed to encode segment for key %s", segment.Key)
	}
	encoded_segment := base64.StdEncoding.EncodeToString(bytes_buffer.Bytes())

	encoded_segment_bytes := []byte(encoded_segment)

	// ------ Write to disk:
	bytes_written, err := file.WriteAt(encoded_segment_bytes, int64(write_offset))
	// Debug3("[%s] bytes written to file [%s] at offset [%s]", strconv.Itoa(bytes_written), file.Name(), strconv.Itoa(int(write_offset)))

	if err != nil {
		panic(err)
	}

	file.Sync()
	file.Close()

	return write_offset, uint64(bytes_written)
}

func PersistIndexMap() {

	// When the memtable gets bigger than some threshold—typically a few megabytes—write it out to disk as an SSTable file.
	// This can be done efficiently because the tree already maintains the key-value pairs sorted by key.
	// The new SSTable file becomes the most recent segment of the database.
	// !!!: While the SSTable is being written out to disk, writes can continue to a new memtable instance.
	Debug0("Persisting index map to a new segment file done.")
}

func Read(key string) (*Segment, bool) {
	// In order to serve a read request, first try to find the key in the memtable,
	index_item, value_exists := index_map[key]
	if value_exists {

		// ------ Open file from disk to read:
		var file *os.File
		if _, err := os.Stat(FILE_PATH); err == nil {
			file, err = os.Open(FILE_PATH)
			if err != nil {
				panic(err)
			}
		} else if errors.Is(err, os.ErrNotExist) {
			Debug1("File [%s] does not exist, creating...", FILE_PATH)
			panic(err)
		} else {
			panic(err)
		}

		// ------ Read from disk into byte buffer
		bytes_buff := make([]byte, index_item.Size)

		_, err := file.ReadAt(bytes_buff, int64(index_item.Offset))
		if err != nil {
			Debug1("Error while reading value for key [%s]", key)
			panic(err)
		}

		// ------ Create a segment
		segment := Segment{}

		// ------ Decode value which was read from the disk
		string_bytes, err := base64.StdEncoding.DecodeString(string(bytes_buff))
		if err != nil {
			Debug1("Error while decoding bsae64 value for key [%s]", key)
			panic(err)
		}
		var bytes_buffer bytes.Buffer
		bytes_buffer.Write(string_bytes)
		decoder := gob.NewDecoder(&bytes_buffer)
		err = decoder.Decode(&segment)
		if err != nil {
			Debug1("Error while decoding gob value for key [%s]", key)
			panic(err)
		}

		return &segment, true
	}

	Debug1("Value does not exist for key [%s]", key)
	return new(Segment), false
}

// Debug1("Value for key [%s] not found in memtable, searching in persisted segments...", key)
// // then in the persisted segment files
// for segment_idx := 0; segment_idx < persisted_segments_num; segment_idx++ {
// 	Debug2("Searching for key [%s] in persisted segment [%s]...", key, strconv.Itoa(segment_idx))
// 	ret_value, value_exists = ReadSegment(segment_idx, key)
// 	if value_exists {
// 		Debug2("Value for key [%s] found in segment [%s], returning.", key, strconv.Itoa(segment_idx))
// 		return ret_value, true
// 	}
// }
// Debug1("Value for key [%s] not found in persisted segments, returning empty value.", key)
// return "", false

func ReadSegment(segment_idx int, key string) (string, bool) {
	Debug1("Reading from segment [%s]...", strconv.Itoa(segment_idx))
	return "from_read_segment", true
}

func Delete() {
}

func Compact() {
	// From time to time, run a merging and compaction process in the background
	// to combine segment files and to discard overwritten or deleted values.
}
