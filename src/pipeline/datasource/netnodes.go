package datasource

import (
	"net"
	"bufio"
)

/*
网络写节点
*/
func NetworkSink(addr string,inChan <- chan int){
	var(
		// 接口类型
		lister net.Listener
		// 接口类型
		err error
		// 接口类型
		conn net.Conn
		// 结构体指针类型
		writer *bufio.Writer
	)
	if lister ,err = net.Listen("tcp",addr); err != nil {
		panic(err)
	}
	go func() {
		// go func() 在将要推出之前依次执行
		// writer.Flush() -> conn.Close() -> lister.Close()
		// 比其他语言 try-catch 灵活好用很多
		defer lister.Close()
		if conn,err = lister.Accept(); err != nil {
			panic(err)
		}
		defer conn.Close()
		writer = bufio.NewWriter(conn)
		defer writer.Flush()
		WriteSink(writer,inChan)
	}()
}

/*
网络数据源
*/
func NetworkSource(addr string) <-chan int {
	var (
		out chan int
		conn net.Conn
		err error
		readChan <-chan int
		v int
	)
	out = make(chan int,8)
	go func(){
		// 连接server
		if conn,err = net.Dial("tcp",addr); err != nil {
			panic(err)
		}
		// TODO close connection
		readChan = SplitBlockReaderSource(bufio.NewReader(conn),-1)
		for v = range readChan {
			out <- v
		}
		close(out)
	}()
	return out
}