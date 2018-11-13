package main

import (
	"os"
	"pipeline/datasource"
	"bufio"
	"fmt"
	"strconv"
)

func main(){
	//externalSortSmallTest()
	//externalSortLargeTest()
	//netExternalSortSmallTest()
	netExternalSortLargeTest()
}

func wtrteToFile(inChan <- chan int,filePath string) {
	file,err := os.Create(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	// defer是先进后出
	// 执行顺序 writer.Flush() -> file.Close()
	defer writer.Flush()
	datasource.WriteSink(writer,inChan)
}

func printFile(filePath string) {
	file,err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	p := datasource.ReaderSource(file)
	limit := 0
	for v := range p {
		fmt.Println(v)
		limit ++
		if limit > 100 {
			break
		}
	}
}

func createPipeline(filePath string,fileSize,chunkCount int) <- chan int {
	// 读取的每一块的大小
	// TODO 产品代码不能整除要处理
	chunkSize := fileSize / chunkCount
	sortResults := [] <- chan int{}
	datasource.Init()
	for i := 0; i < chunkCount ; i++ {
		// file不要 defer close 后面要用
		// TODO 用完file指针应该在外部close掉 这里不实现
		// WARNING NOTICE 这里必须在for循环内部打开文件
		// WARNING NOTICE 不能在for循环外打开文件 否则丢数据 Seek的缘故
		file,err := os.Open(filePath)
		if err != nil {
			panic(err)
		}
		// 从第i块的 0-头部 开始读取
		file.Seek(int64(i * chunkSize),0)
		splitSource := datasource.SplitBlockReaderSource(bufio.NewReader(file),chunkSize)
		sortResults = append(sortResults,datasource.InMemSort(splitSource))
	}
	return datasource.MergeN(sortResults...)
}

func externalSortSmallTest(){
	var(
		filePath string
		fileSize int
		chunkCount int
	)
	filePath = datasource.EXTERNAL_SORT_SMALL_IN_FILE
	fileSize = datasource.EXTERNAL_SORT_SMALL_IN_FILE_SIZE
	chunkCount = datasource.EXTERNAL_SORT_CHUNK_COUNT

	printFile(filePath)
	fmt.Println("-----------------")

	// 一边运行pipeline
	p := createPipeline(filePath,fileSize,chunkCount)
	// 一边写文件
	fileOutPath := datasource.EXTERNAL_SORT_SMALL_OUT_FILE
	wtrteToFile(p,fileOutPath)
	// 打印最终排序结果
	printFile(fileOutPath)
}

func externalSortLargeTest(){
	var(
		filePath string
		fileSize int
		chunkCount int
	)
	filePath = datasource.EXTERNAL_SORT_LARGE_IN_FILE
	fileSize = 8 * datasource.EXTERNAL_SORT_LARGE_IN_FILE_SIZE
	chunkCount = datasource.EXTERNAL_SORT_CHUNK_COUNT

	// 一边运行pipeline
	p := createPipeline(filePath,fileSize,chunkCount)
	// 一边写文件
	fileOutPath := datasource.EXTERNAL_SORT_LARGE_OUT_FILE
	wtrteToFile(p,fileOutPath)
	// 打印最终排序结果
	printFile(fileOutPath)
}

// TODO 网络版外部排序

func createNetworkPipeline(filePath string,fileSize,chunkCount int) <- chan int {
	chunkSize := fileSize / chunkCount
	sortAddr := []string{}
	datasource.Init()
	for i := 0; i < chunkCount ; i++ {
		file,err := os.Open(filePath)
		if err != nil {
			panic(err)
		}
		file.Seek(int64(i * chunkSize),0)
		splitSource := datasource.SplitBlockReaderSource(bufio.NewReader(file),chunkSize)
		addr := ":" + strconv.Itoa(7000 + i)
		// InMemSort(splitSource) 做完内部排序
		// 开启1个server
		datasource.NetworkSink(addr,datasource.InMemSort(splitSource))
		sortAddr = append(sortAddr,addr)
	}

	// TODO REST server
	// return nil

	sortResults := []<-chan int{}
	for _,addr := range sortAddr {
		// 连接上面开启的server
		sortResults = append(sortResults,datasource.NetworkSource(addr))
	}
	return datasource.MergeN(sortResults...)
}

func netExternalSortSmallTest(){
	var(
		filePath string
		fileSize int
		chunkCount int
	)
	filePath = datasource.EXTERNAL_SORT_SMALL_IN_FILE
	fileSize = 8 * datasource.EXTERNAL_SORT_SMALL_IN_FILE_SIZE
	chunkCount = datasource.EXTERNAL_SORT_CHUNK_COUNT

	printFile(filePath)
	fmt.Println("-----------------")

	// 一边运行pipeline
	p := createNetworkPipeline(filePath,fileSize,chunkCount)
	// 一边写文件
	fileOutPath := datasource.EXTERNAL_SORT_SMALL_OUT_FILE
	wtrteToFile(p,fileOutPath)
	// 打印最终排序结果
	printFile(fileOutPath)
}

func netExternalSortLargeTest(){
	var(
		filePath string
		fileSize int
		chunkCount int
	)
	filePath = datasource.EXTERNAL_SORT_LARGE_IN_FILE
	fileSize = 8 * datasource.EXTERNAL_SORT_LARGE_IN_FILE_SIZE
	chunkCount = datasource.EXTERNAL_SORT_CHUNK_COUNT

	// 一边运行pipeline
	p := createNetworkPipeline(filePath,fileSize,chunkCount)
	// 一边写文件
	fileOutPath := datasource.EXTERNAL_SORT_LARGE_OUT_FILE
	wtrteToFile(p,fileOutPath)
	// 打印最终排序结果
	printFile(fileOutPath)
}