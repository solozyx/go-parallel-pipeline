package datasource

import (
	"sort"
	"io"
	"encoding/binary"
	"math/rand"
	"time"
	"fmt"
)

var(
	startTime time.Time
)

func Init() {
	startTime = time.Now()
}

/*
测试数据源
a 可变长参数 ...int
*/
func ArraySource(a ...int) (<- chan int) {
	out := make(chan int)
	go func(){
		for _,v := range a {
			out <- v
		}
		// 一般channel不需要close
		// 但是这里数据组装完毕 数据有明显结尾 close
		close(out)
	}()
	return out
}

/*
内部排序-升序
*/
func InMemSort(inChan <-chan int) <- chan int {
	//out := make(chan int)
	// 给channel增加buffer
	out := make(chan int,100)
	go func() {
		// read into memory
		a := []int{}
		for v := range inChan {
			// slice是不可变对象append后要接收新的返回值
			a = append(a,v)
		}
		fmt.Println("Read done : ",time.Now().Sub(startTime))
		// 内部排序-升序
		sort.Ints(a)
		fmt.Println("InMemSort done : ",time.Now().Sub(startTime))
		// output
		for _,v := range a {
			out <- v
		}
		close(out)
	}()
	return out
}

/*
归并节点-外部排序最核心节点
*/
func Merge(inChan1,inChan2 <- chan int) <- chan int {
	//out := make(chan int)
	// 给channel增加buffer
	out := make(chan int,100)
	go func(){
		// 同时从 inChan1 inChan2 获取数据
		// 比较大小
		// 2个chan的数据量不一定相同
		v1,ok1 := <- inChan1
		v2,ok2 := <- inChan2
		for ok1 || ok2 {
			// inChan2没数据
			// inChan1有数据 v1 <= v2
			if !ok2 || (ok1 && v1 <= v2) {
				out <- v1
				// update v1
				v1,ok1 = <- inChan1
			} else {
				out <- v2
				v2,ok2 = <- inChan2
			}
		}
		close(out)
		fmt.Println("Merge done : ",time.Now().Sub(startTime))
	}()
	return out
}

/*
从 io.Reader 读取数据
*/
func ReaderSource(reader io.Reader) <-chan int {
	//out := make(chan int)
	// 给channel增加buffer
	out := make(chan int,100)
	go func() {
		// golang 的 int 根据操作系统 64位机 int就是64位
		buffer := make([]byte,8) // byte * 8 = 64位int
		for {
			// 返回 读取到的byte数 error 读到EOF则有错误
			// 没读取8byte 读到4byte就EOF
			n,err := reader.Read(buffer)
			// 读取到数据
			if n > 0 {
				// binary.LittleEndian
				// binary.BigEndian 高位在高字节
				// 读写统一即可
				// 无符号
				// binary.BigEndian.Uint64()
				// 转成有符号的int
				v := int(binary.BigEndian.Uint64(buffer))
				out <- v
			}
			if err != nil {
				break
			}
		}
		close(out)
	}()
	return out
}

/*
最终排序结果落盘
*/
func WriteSink(writer io.Writer,inChan <-chan int) {
	for v := range inChan {
		buffer := make([]byte,8)
		binary.BigEndian.PutUint64(buffer,uint64(v))
		writer.Write(buffer)
	}
}

/*
随机数据源
*/
func RandomSource(count int) <- chan int {
	out := make(chan int)
	go func() {
		for i :=0 ; i < count; i++ {
			out <- rand.Int()
		}
		close(out)
	}()
	return out
}

/*
归并N个inputs
*/
func MergeN(inputs ... <- chan int) <- chan int {
	if len(inputs) == 1 {
		return inputs[0]
	}
	middle := len(inputs) / 2
	// TODO 递归
	// inputs[0...m) inputs[m...end)
	return  Merge(
		MergeN(inputs[:middle]...),
		MergeN(inputs[middle:]...))
}

/*
分块读取大文件
*/
func SplitBlockReaderSource(reader io.Reader, chunkSize int) <-chan int {
	// out := make(chan int)
	// 给channel增加buffer
	out := make(chan int,100)

	go func() {
		buffer := make([]byte,8)
		bytesRead := 0
		for {
			n,err := reader.Read(buffer)
			bytesRead += n
			if n > 0 {
				v := int(binary.BigEndian.Uint64(buffer))
				out <- v
			}
			// chunkSize == -1 表示不分块读取
			if err != nil || (chunkSize != -1 && bytesRead >= chunkSize){
				break
			}
		}
		close(out)
	}()
	return out
}