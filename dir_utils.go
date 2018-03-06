package sequencefile

import (
	"sync"
	"path/filepath"
	"os"
	"sort"
	"github.com/jimuyida/glog"
)

func walkDir(dir string, wg *sync.WaitGroup, fileSizes chan<- int64) {
	defer wg.Done()
	for _, entry := range dirents(dir) {
		if entry.IsDir() {//目录
			wg.Add(1)
			subDir := filepath.Join(dir, entry.Name())
			go walkDir(subDir, wg, fileSizes)
		} else {
			fileSizes <- entry.Size()
		}
	}
}

//sema is a counting semaphore for limiting concurrency in dirents
var sema = make(chan struct{}, 20)
//按修改时间倒序
//ioutil的按名称升序
func ReadDir(dirname string) ([]os.FileInfo, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	list, err := f.Readdir(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ModTime().Nanosecond() > list[j].ModTime().Nanosecond() })
	return list, nil
}

//读取目录dir下的文件信息
func dirents(dir string) []os.FileInfo {
	sema <- struct{}{}
	defer func() { <-sema }()
	entries, err := ReadDir(dir)
	if err != nil {
		glog.Errorf("du: %v\n", err)
		return nil
	}
	return entries
}

//输出文件数量的大小
func printDiskUsage(nfiles, nbytes int64) {
	glog.Infof("%d files %.1f GB\n", nfiles, float64(nbytes)/1e9)
}

func GetFileSize(root string) (num int64,size int64){
	fileSizes := make(chan int64)
	var wg sync.WaitGroup
	wg.Add(1)
	go walkDir(root, &wg, fileSizes)

	go func() {
		wg.Wait() //等待goroutine结束
		close(fileSizes)
	}()
	var nfiles, nbytes int64
loop:
	for {
		select {
		case size, ok := <-fileSizes:
			if !ok {
				break loop
			}
			nfiles++
			nbytes += size
		}
	}


	return nfiles,nbytes
}
