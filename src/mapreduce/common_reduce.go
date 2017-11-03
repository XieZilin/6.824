package mapreduce

import (
	"os"
	"encoding/json"
	"log"
	"sort"
	"fmt"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	var decoders []* json.Decoder
	for i:=0; i < nMap; i++ {
		fileNameI := reduceName(jobName, i, reduceTaskNumber)
		fd, err := os.OpenFile(fileNameI, os.O_RDONLY, 0600)
		if err != nil {
			log.Fatal("check: ", err)
		}
		defer fd.Close()
		decoders = append(decoders, json.NewDecoder(fd))
	}
	
	keyValues := make(map[string][]string)
	for i:=0; i < nMap; i++ {
		var kv KeyValue
		for {
			err := decoders[i].Decode(&kv)
			if err != nil {
				break
			}
			keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
		}
	}  
	fmt.Println("Size of keyValues, ", len(keyValues))
	var keys []string
	for k := range keyValues {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	
	fd, err := os.OpenFile(outFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatal("check: ", err)
	}
	defer fd.Close()

	encoder := json.NewEncoder(fd)
	// a -> count
	for _, key := range keys {
		encoder.Encode(KeyValue{key, reduceF(key, keyValues[key])})
	}
}
