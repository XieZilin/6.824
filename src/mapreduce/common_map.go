package mapreduce

import (
	"bytes"
	"hash/fnv"
	"os"
	"encoding/json"
	"log"
)

// doMap manages one map task: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// mapF "a b c" -> [a-> "", b-> "", c->""]
	output, err := os.Open(inFile)
	if err != nil {
		log.Fatal("check: ", err)
	}
	defer output.Close()
	var buffer bytes.Buffer
	buffer.ReadFrom(output)
	keyValue := mapF(inFile, buffer.String())
	
	var encoders []* json.Encoder
	for i:=0; i < nReduce; i++ {
		fileNameI := reduceName(jobName, mapTaskNumber, i)
		fd, err := os.OpenFile(fileNameI, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			log.Fatal("check: ", err)
		}
		defer fd.Close()
		encoders = append(encoders, json.NewEncoder(fd))
	}
	for _, kv := range keyValue {
		idx := ihash(kv.Key) % nReduce
		err = encoders[idx].Encode(kv)
		if err != nil {
			log.Fatal("check: ", err)
		}
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
