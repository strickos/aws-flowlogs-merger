package data

import (
	"sync"
	"time"
)

/*
FileToProcessInfo describes an S3 file to process and the waitgroup to notify when done!
*/
type FileToProcessInfo struct {
	Bucket       string `json:"bucket"`
	Key          string `json:"key"`
	Size         int64  `json:"size"`
	TimestampVal int64  `json:"timestamp"`

	UncompressedSize int64     `json:"-"`
	Timestamp        time.Time `json:"-"`
	wg               *sync.WaitGroup
}

/*
MakeFileToProcessInfo builds a new FileToProcessInfo object and returns it.
*/
func MakeFileToProcessInfo(bucket string, key string, size int64, wg *sync.WaitGroup) *FileToProcessInfo {
	info := FileToProcessInfo{
		Bucket: bucket,
		Key:    key,
		Size:   size,
		wg:     wg,
	}
	return &info
}

/*
Done marks this file as processed.
*/
func (f *FileToProcessInfo) Done() {
	f.wg.Done()
}
