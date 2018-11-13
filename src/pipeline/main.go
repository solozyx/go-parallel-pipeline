package main

import (
	"os"
	"pipeline/datasource"
	"fmt"
	"bufio"
)

func splitReadFile() {
	var (
		filepath string
	)
	filepath = "F:/_gotestdata/largedata.in"
	file,err := os.Open(filepath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	readerSource := datasource.SplitBlockReaderSource(bufio.NewReader(file),-1)
	limit := 0
	for v := range readerSource {
		fmt.Println(int(v))
		limit ++
		if limit >= 100 {
			break
		}
	}
}

func main() {
	splitReadFile()
}