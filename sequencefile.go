package sequencefile

import (
	MMAP "github.com/edsrzf/mmap-go"
	"os"
	"fmt"
	"sync"
	"path/filepath"
	"io/ioutil"
	"runtime"
	"syscall"
)

const (
	BytesWritableClassName = "org.apache.hadoop.io.BytesWritable"
	TextClassName          = "org.apache.hadoop.io.Text"
	IntWritableClassName   = "org.apache.hadoop.io.IntWritable"
	LongWritableClassName  = "org.apache.hadoop.io.LongWritable"
)

type Header struct {
	Version                   byte
	KeyClassName              string
	ValueClassName            string
	ValueCompression          bool //1字节
	BlockCompression          bool //1字节
	CompressionCodecClassName string
	Metadata                  map[string]string
	SyncMarker                string //16字节
}

type SequenceFile struct {
	destFile *MmapStruct
	header   *Header
}

//读取目录dir下的文件信息
func (sequenceFile *SequenceFile) dirents(dir string) []os.FileInfo {
	sema <- struct{}{}
	defer func() { <-sema }()
	entries, err := ioutil.ReadDir(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "du: %v\n", err)
		return nil
	}
	return entries
}

func (sequenceFile *SequenceFile) walkDir(dir string, subdir string, wg *sync.WaitGroup) {
	defer wg.Done()
	for _, entry := range sequenceFile.dirents(dir + "/" + subdir) {
		if entry.IsDir() { //目录
			wg.Add(1)
			subDir := filepath.Join(subdir, entry.Name())
			sequenceFile.walkDir(dir, subDir, wg)
		} else {
			err := sequenceFile.writeFile(dir, filepath.Join(subdir, entry.Name()))
			if err != nil {
				panic(err)
			}
		}
	}
}

func (sequenceFile *SequenceFile) Dir2Seq(dir string,seq string) (error){
	var wg sync.WaitGroup
	wg.Add(1)
	num, sz := GetFileSize(dir)
	fmt.Printf("%s 有 %d 个文件，一共 %.1f GB\n", dir, num, float64(sz)/1e9)

	dst, err := os.OpenFile(seq, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	err = dst.Truncate(10*1024*1024 + sz + num*20 + num*256)
	if err != nil {
		return err
	}
	dstMap, err := MMAP.Map(dst, MMAP.RDWR, 0)
	if err != nil {
		return err
	}
	defer func() {
		if err := dstMap.Unmap(); err != nil {
			//fmt.Println("error unmapping: %s", err)
		}
	}()
	sequenceFile.header = &Header{}
	sequenceFile.header.Version = 6
	sequenceFile.header.KeyClassName = TextClassName
	sequenceFile.header.ValueClassName = BytesWritableClassName
	sequenceFile.header.ValueCompression = false
	sequenceFile.header.BlockCompression = false
	sequenceFile.header.Metadata = make(map[string]string)
	sequenceFile.header.Metadata["name"] = "name1"
	sequenceFile.header.Metadata["dir"] = dir
	sequenceFile.header.Metadata["value"] = "name.............................1"
	sequenceFile.destFile = NewMmapStruct(dstMap)
	err = sequenceFile.writeHeader()
	if err != nil {
		return err
	}
	go sequenceFile.walkDir(dir, "", &wg)
	wg.Wait()
	err = dstMap.Unmap()
	if err != nil {
		return err
	}
	len := sequenceFile.destFile.writePos
	err = dst.Truncate(len)
	return err
}

func (sequenceFile *SequenceFile) writeFile(dir string, file string) (error){
	fmt.Println(dir + "\\" + file)
	f, err := os.OpenFile(dir+"\\"+file, os.O_RDONLY, 0644)
	defer f.Close()
	mmap, err := MMAP.Map(f, MMAP.RDONLY, 0)
	if err != nil {
		return err
	}
	defer func() {
		if err := mmap.Unmap(); err != nil {
			fmt.Println("error unmapping: %s", err)
		}
	}()
	fi, err := f.Stat()
	if runtime.GOOS == "windows" {
		pointer, err := syscall.UTF16PtrFromString(dir+"\\"+file)
		if err != nil {
			return err
		}
		attributes, err := syscall.GetFileAttributes(pointer)
		if err != nil {
			return err
		}
		if attributes&syscall.FILE_ATTRIBUTE_HIDDEN != 0 ||  attributes&syscall.FILE_ATTRIBUTE_SYSTEM != 0{
			fmt.Printf("忽略隐藏与系统文件: %s\n", dir+"\\"+file)
			return nil
		}
	}
	keyLength := sequenceFile.destFile.GetStringLen(file)
	valLength := int(fi.Size())

	if valLength == 0 {
		fmt.Printf("忽略空文件: %s\n", dir+"\\"+file)
		return nil
		//return fmt.Errorf("")
	}

	fi.Mode()

	err = sequenceFile.destFile.Sync()
	if err != nil {
		return err
	}
	err = sequenceFile.destFile.WriteInt(valLength+keyLength+4, true) //key value总 长度
	if err != nil {
		return err
	}
	//fmt.Println(valLength + keyLength + 4)
	err = sequenceFile.destFile.WriteInt(keyLength, true) //key 长度
	if err != nil {
		return err
	}
	err = sequenceFile.destFile.WriteString(file, true)
	if err != nil {
		return err
	}
	err = sequenceFile.destFile.WriteInt(valLength, true) //文件长度
	if err != nil {
		return err
	}

	var BATCH_SIZE int64 = 10 * 1024 * 1024
	var i int64
	for i = 0; i < fi.Size(); i++ {
		size := BATCH_SIZE
		if (BATCH_SIZE > (fi.Size() - i)) {
			size = fi.Size() - i - 1
		}
		//fmt.Println("%d:%d",i,size)
		err = sequenceFile.destFile.Write(mmap[i:i+size+1], true)
		if err != nil {
			return err
		}
		i += size;
		//mmap.Flush()
	}
	//fmt.Println("%d:%d",i,fi.Size())
	return nil
}

func (sequenceFile *SequenceFile) writeHeader() (error){
	err := sequenceFile.destFile.Write([]byte("SEQ"), true)
	if err != nil {
		return err
	}
	err = sequenceFile.destFile.WriteByte(sequenceFile.header.Version, true)
	if err != nil {
		return err
	}
	err = sequenceFile.destFile.WriteString(sequenceFile.header.KeyClassName, true)
	if err != nil {
		return err
	}
	err = sequenceFile.destFile.WriteString(sequenceFile.header.ValueClassName, true)
	if err != nil {
		return err
	}
	err = sequenceFile.destFile.WriteBoolean(sequenceFile.header.ValueCompression)
	if err != nil {
		return err
	}
	err = sequenceFile.destFile.WriteBoolean(sequenceFile.header.BlockCompression)
	if err != nil {
		return err
	}
	err = sequenceFile.destFile.WriteString(sequenceFile.header.CompressionCodecClassName, true)
	if err != nil {
		return err
	}
	err = sequenceFile.destFile.WriteMetadata(sequenceFile.header.Metadata)
	if err != nil {
		return err
	}
	err = sequenceFile.destFile.WriteString(sequenceFile.header.SyncMarker, true)
	if err != nil {
		return err
	}
	return nil
}