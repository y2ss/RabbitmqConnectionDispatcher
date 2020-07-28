package common

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func ProtobufPrefixCutout(cmd string) string {
	return cmd[16:]
}

func AssembleBytes(b1, b2 []byte) []byte {
	npl := make([]byte, len(b1) + len(b2))
	for i, b := range b1 {
		npl[i] = b
	}
	for i, b := range b2 {
		npl[i + len(b1)] = b
	}
	return npl
}

func PathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil || os.IsExist(err)
}

func StringToTime(t string) time.Time {
	loc, _ := time.LoadLocation("Local")
	tmp, _ := time.ParseInLocation("2006-01-02 15:04:05", t, loc)
	return tmp
}

func TimestampToYYYYMMDD(timestamp int64) string {
	return time.Unix(timestamp, 0).Format("20060102")
}

func GetGoroutineID() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}

func CalcCrc16(data []byte, len uint32) uint16 {
	var wCRCin uint16 = 0x0000
	var wCPoly uint16 = 0x1021
	var wChar uint8 = 0
	var idx uint32 = 0
	for ; idx < len; idx++ {
		wChar = data[idx]
		wCRCin ^= (uint16(wChar) << 8)
		for i := 0; i < 8; i++ {
			if (wCRCin & 0x8000) != 0 {
				wCRCin = (wCRCin << 1) ^ wCPoly
			} else {
				wCRCin = wCRCin << 1
			}
		}
	}
	return wCRCin
}

func TimestampFix(ts int64) int64 {
	return ts - 28800//- 8 hours
}

func ScheduleWork(d time.Duration, f func() bool) {
	for {
		if f() {
			break
		} else {
			now := time.Now()
			next := now.Add(d)
			next = time.Date(next.Year(), next.Month(), next.Day(), next.Hour(), next.Minute(), next.Second(), 0, next.Location())
			t := time.NewTimer(next.Sub(now))
			<- t.C
			t.Stop()
		}
	}
}

func ScheduleWorkAsync(d time.Duration, f func() bool) {
	go func() {
		for {
			if f() {
				break
			} else {
				now := time.Now()
				next := now.Add(d)
				next = time.Date(next.Year(), next.Month(), next.Day(), next.Hour(), next.Minute(), next.Second(), 0, next.Location())
				t := time.NewTimer(next.Sub(now))
				<- t.C
				t.Stop()
			}
		}
	}()
}

func DoingAfter(d time.Duration, f func()) {
	now := time.Now()
	next := now.Add(d)
	next = time.Date(next.Year(), next.Month(), next.Day(), next.Hour(), next.Minute(), next.Second(), 0, next.Location())
	t := time.NewTimer(next.Sub(now))
	<- t.C
	t.Stop()
	f()
}

func DoingAfterAsync(d time.Duration, f func()) {
	go func() {
		now := time.Now()
		next := now.Add(d)
		next = time.Date(next.Year(), next.Month(), next.Day(), next.Hour(), next.Minute(), next.Second(), 0, next.Location())
		t := time.NewTimer(next.Sub(now))
		<- t.C
		t.Stop()
		f()
	}()
}

//压缩 使用gzip压缩成tar.gz
func Compress(files []*os.File, dest string) error {
	d, _ := os.Create(dest)
	defer d.Close()
	gw := gzip.NewWriter(d)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()
	for _, file := range files {
		err := compress(file, "", tw)
		if err != nil {
			return err
		}
	}
	return nil
}

func compress(file *os.File, prefix string, tw *tar.Writer) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.IsDir() {
		prefix = prefix + "/" + info.Name()
		fileInfos, err := file.Readdir(-1)
		if err != nil {
			return err
		}
		for _, fi := range fileInfos {
			f, err := os.Open(file.Name() + "/" + fi.Name())
			if err != nil {
				return err
			}
			err = compress(f, prefix, tw)
			if err != nil {
				return err
			}
		}
	} else {
		header, err := tar.FileInfoHeader(info, "")
		header.Name = prefix + "/" + header.Name
		if err != nil {
			return err
		}
		err = tw.WriteHeader(header)
		if err != nil {
			return err
		}
		_, err = io.Copy(tw, file)
		file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
