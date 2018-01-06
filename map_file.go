package sequencefile

import (
	"sync"
	MMAP "github.com/edsrzf/mmap-go"
	"crypto/md5"
	"io"
	"github.com/satori/go.uuid"
	"time"
)

type MmapStruct struct {
	mmap        MMAP.MMap
	writePos    int64
	locker      sync.Mutex
	isFirstFile bool
	sync        []byte
}

func NewMmapStruct(mmp MMAP.MMap) (*MmapStruct) {
	return &MmapStruct{
		mmap:        mmp,
		isFirstFile: true,
	}
}

func (mmap *MmapStruct) writeVInt(i int) (error) {
	return mmap.writeVLong(int64(i))
}

func (mmap *MmapStruct) writeVLong(i int64) (error) {
	wcount := 0
	if (i >= -112 && i <= 127) {
		return mmap.WriteByte(byte(i), false)
	} else {
		var len int = -112;
		if (i < 0) {
			i = ^i;
			len = -120;
		}

		for tmp := i; tmp != 0; {
			tmp = tmp >> 8
			len--
		}

		err := mmap.WriteByte(byte(len), false);
		if err != nil {
			return err
		}
		wcount++
		if len < -120 {
			len = -(len + 120)
		} else {
			len = -(len + 112)
		}

		for idx := len; idx != 0; {
			var shiftbits uint64 = uint64(idx-1) * 8;
			var mask1 int64 = 255
			mask1 = mask1 << shiftbits
			ii := int(i & mask1)
			ii = ii >> shiftbits
			err = mmap.WriteByte(byte(ii), false)
			if err != nil {
				return err
			}
			wcount++

			idx--
		}
	}
	return nil
}

func (mmap *MmapStruct) GetIntAsVIntLen(i int64) (int) {
	wcount := 0
	if (i >= -112 && i <= 127) {
		return 1
	} else {
		var len int = -112;
		if (i < 0) {
			i = ^i;
			len = -120;
		}
		for tmp := i; tmp != 0; {
			tmp = tmp >> 8
			len--
		}
		wcount++
		if len < -120 {
			len = -(len + 120)
		} else {
			len = -(len + 112)
		}
		for idx := len; idx != 0; {
			var shiftbits uint64 = uint64(idx-1) * 8;
			var mask1 int64 = 255
			mask1 = mask1 << shiftbits
			ii := int(i & mask1)
			ii = ii >> shiftbits
			wcount++
			idx--
		}
	}
	return wcount

}

func (mmap *MmapStruct) WriteBoolean(p bool) ( error) {
	var b byte = 0
	if p {
		b = 1
	}
	err := mmap.WriteByte(b, false)
	if err != nil {
		return err
	}
	return nil
}

func (mmap *MmapStruct) GetStringLen(p string) (n int) {
	if len(p) == 0 {
		return 0
	}
	num := mmap.GetIntAsVIntLen(int64(len(p)))

	return num + len(p)
}

func (mmap *MmapStruct) WriteString(p string, needLock bool) ( error) {
	if needLock {
		mmap.locker.Lock()
		defer mmap.locker.Unlock()
	}

	if len(p) == 0 {
		return nil
	}
	err := mmap.writeVInt(len(p))
	if err != nil {
		return err
	}
	err = mmap.Write([]byte(p), false)
	if err != nil {
		return err
	}
	//mmap.mmap[mmap.writePos] = p;
	//mmap.writePos++;
	return nil
}

func (mmap *MmapStruct) Sync() (err error) {
	mmap.locker.Lock()
	defer mmap.locker.Unlock()

	if !mmap.isFirstFile {
		err := mmap.WriteInt(-1, false)
		if err != nil {
			return err
		}
	} else {
		mmap.isFirstFile = false
		uid := uuid.NewV4()
		//uid,err = uuid.FromString(boltInfo.CheckSum)
		//fmt.Printf("UUIDv4: %s\n", uid)
		w := md5.New()
		io.WriteString(w, uid.String()+"@"+time.Now().Format("2006-01-02_03_04_05_PM")) //将str写入到w中
		//md5str2: = fmt.Sprintf("%x", w.Sum(nil))  //w.Sum(nil)将w的hash转成[]byte格式
		//fmt.Println(mdtstr2)
		mmap.sync = make([]byte, 16)
		copy(mmap.sync, w.Sum(nil))
	}

	return mmap.Write(mmap.sync, false)
}

func (mmap *MmapStruct) WriteByte(p byte, needLock bool) (error) {
	if needLock {
		mmap.locker.Lock()
		defer mmap.locker.Unlock()
	}
	mmap.mmap[mmap.writePos] = p;
	mmap.writePos++;
	return  nil
}

func (mmap *MmapStruct) Write(p []byte, needLock bool) (error) {
	if needLock {
		mmap.locker.Lock()
		defer mmap.locker.Unlock()
	}
	for i := 0; i < len(p); i++ {
		mmap.mmap[mmap.writePos] = p[i]
		mmap.writePos++
	}
	return nil
}

func (mmap *MmapStruct) WriteInt(v int, needLock bool) ( error) {
	if needLock {
		mmap.locker.Lock()
		defer mmap.locker.Unlock()
	}
	err := mmap.WriteByte((byte)(0xff&(v>>24)), false)
	if err != nil {
		return err
	}
	err = mmap.WriteByte((byte)(0xff&(v>>16)), false)
	if err != nil {
		return err
	}
	err = mmap.WriteByte((byte)(0xff&(v>>8)), false)
	if err != nil {
		return err
	}
	err = mmap.WriteByte((byte)(0xff&(v>>0)), false)
	if err != nil {
		return err
	}
	return nil
}

func (mmap *MmapStruct) WriteMetadata(meta map[string]string) ( error) {
	mmap.locker.Lock()
	defer mmap.locker.Unlock()
	mmap.WriteInt(len(meta), false)
	for key, value := range meta {
		e := mmap.WriteString(key, false)
		if e != nil {
			return e
		}
		e = mmap.WriteString(value, false)
		if e != nil {
			return e
		}
	}
	return nil
}