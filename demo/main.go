package main

import (
	"pipeline/datasource"
	"fmt"
	"os"
	"bufio"
)

func fordemo(){
	sourceChan := datasource.ArraySource(3,2,6,7,4)
	for {
		// channel 有实际数据 ok==true 否则 ok==false
		if num,ok := <- sourceChan; ok {
			fmt.Println(num)
		} else {
			break
		}
	}
}

func forrangedemo()  {
	sourceChan := datasource.ArraySource(3,2,6,7,4)
	for num := range sourceChan {
		fmt.Println(num)
	}
}

func mergeDemo() {
	sourceChan := datasource.Merge(
		datasource.InMemSort(datasource.ArraySource(3,2,6,7,4)),
		datasource.InMemSort(datasource.ArraySource(7,4,0,3,2,13,8)))
	for num := range sourceChan {
		fmt.Println(num)
	}
}

func createFile() {
	var (
		filepath string
		numCount int
	)
	// filepath = "./_gotestdata/small.in"
	// numCount = 32

	filepath = datasource.EXTERNAL_SORT_LARGE_IN_FILE
	numCount = datasource.EXTERNAL_SORT_LARGE_IN_FILE_SIZE

	// 写文件
	file, err := os.Create(filepath)
	if err != nil {
		panic(err)
	}
	// 在函数退出之前执行defer后的语句
	defer file.Close()

	randomSource := datasource.RandomSource(numCount)
	// *File 就是 io.Writer
	// sourcedata.WriteSink(file,randomSource) 生成 800MB 数据非常慢
	// 用 bufio.NewWriter(file) 包装 默认的buffer size
	// sourcedata.WriteSink(bufio.NewWriter(file), randomSource)
	// bufio需要flush 把缓存区的数据落盘
	writer := bufio.NewWriter(file)
	datasource.WriteSink(writer, randomSource)
	writer.Flush()
}

func readFile() {
	var (
		filepath string
	)
	// 小数据 测试
	filepath = "./_gotestdata/small.in"

	// 大数据
	// filepath = "./_gotestdata/largedata.in"

	//读文件
	file,err := os.Open(filepath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// *File 就是 io.Reader
	// readerSource := datasource.ReaderSource(file)
	// 用 bufio.NewReader(file) 包装 默认的buffer size
	readerSource := datasource.ReaderSource(bufio.NewReader(file))
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
	//fordemo()
	//forrangedemo()
	//mergeDemo()
	createFile()
	//readFile()
}