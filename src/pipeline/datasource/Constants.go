package datasource

const(
	EXTERNAL_SORT_CHUNK_COUNT = 4

	EXTERNAL_SORT_SMALL_IN_FILE = "./_gotestdata/small.in"
	EXTERNAL_SORT_SMALL_IN_FILE_SIZE = 32

	EXTERNAL_SORT_SMALL_OUT_FILE = "./_gotestdata/small.out"

	EXTERNAL_SORT_LARGE_IN_FILE = "./_gotestdata/largedata.in"
	// 100MB * 8 = 800MB 大文件 生成的非常慢
	EXTERNAL_SORT_LARGE_IN_FILE_SIZE = 1 * 100 * 1000 * 1000

	EXTERNAL_SORT_LARGE_OUT_FILE = "./_gotestdata/largedata.out"
)