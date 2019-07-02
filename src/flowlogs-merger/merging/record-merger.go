package merging

import (
	"context"
	"flowlogs-merger/data"
	"flowlogs-merger/util"
	"log"
	"sync"
	"time"

	"github.com/xitongsys/parquet-go/source"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/text/message"

	"github.com/satori/go.uuid"

	s3Parquet "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// WriterInfo is used for the writer channel
type WriterInfo struct {
	RecordMap           map[string]*data.LogToProcess
	Timestamp           time.Time
	NumRecords          int64
	WritableRecords     int64
	totalRecordsWritten int64
}

/*
RecordMerger merges as many records as possible into a single file over the period of time that it's processing.
Once it has enough data to form a complete file it'll write that file to S3 and begin building a new file.
*/
type RecordMerger struct {
	records       <-chan *data.LogToProcess
	forHour       int
	writeToBucket string
	writeToPath   string
	deadline      time.Time
	writerChannel chan *WriterInfo
	workerWG      sync.WaitGroup
	recordTracker *RecordTracker

	recordMap           map[string]*data.LogToProcess
	recordCounter       int64
	writableCounter     int64
	aRecord             *data.LogEntry
	totalRecordsWritten int64
	launchedWriterLoop  bool
}

/*
MakeRecordMerger creates a RecordMerger worker and returns it.
*/
func MakeRecordMerger(forHour int, deadline time.Time, writeToBucket string, writeToPath string, records <-chan *data.LogToProcess, recordTracker *RecordTracker) (*RecordMerger, *sync.WaitGroup) {
	mergerer := RecordMerger{
		forHour:       forHour,
		deadline:      deadline,
		records:       records,
		writeToBucket: writeToBucket,
		writeToPath:   writeToPath,
		writerChannel: make(chan *WriterInfo),
		recordTracker: recordTracker,
	}

	mergerer.recordMap = make(map[string]*data.LogToProcess)
	mergerer.launchedWriterLoop = false
	mergerer.workerWG.Add(1)
	return &mergerer, &mergerer.workerWG
}

/*
Run starts the main processing loop
*/
func (m *RecordMerger) Run() {
	maxTimeBetweenWrites, _ := time.ParseDuration("2s")
	recordChannelClosed := false
	for !recordChannelClosed {
		select {
		case record, ok := <-m.records:
			if ok {
				m.processRecord(record)
			} else {
				recordChannelClosed = true
			}
		case <-time.After(maxTimeBetweenWrites):
			m.writeIfNeeded()
		}
	}

	if util.DebugLoggingEnabled {
		log.Printf("Record Merger for hour %d is stopping...", m.forHour)
	}
	if m.writableCounter > 0 {
		if util.DebugLoggingEnabled {
			log.Printf("Will write remaining %d entries (From %d records) to the parquet file", m.writableCounter, m.recordCounter)
		}

		m.writeIfNeeded()
	}

	// Close the Writer Channel - no more records coming...
	close(m.writerChannel)

	if !m.launchedWriterLoop {
		// Writer loop hasn't started because no records have been processed, so mark the writer loop as done
		log.Println("Marking Writer loop as done (there were no records processed)...")
		m.workerWG.Done()
	}
}

func (m *RecordMerger) processRecord(record *data.LogToProcess) {
	if !m.launchedWriterLoop {
		m.launchedWriterLoop = true
		m.aRecord = record.Log
		go m.writerLoop()
	}

	// Increment the record counters
	m.recordCounter++
	m.totalRecordsWritten++

	existingFlow := m.recordMap[record.Log.FlowID]
	if existingFlow != nil {
		// Merge the two flow records...
		existingFlow.Combine(record)
	} else {
		// Add the flow record to the map...
		m.writableCounter++
		m.recordMap[record.Log.FlowID] = record
	}

	tryWrite := m.writableCounter >= 250000
	if m.writableCounter%50000 == 0 {
		// Every 50,000 records, check the state of memory, and decide if we need to short circuit or not
		if util.CurrentMemoryInfo().Alloc > 2147483648 { // 2GB
			tryWrite = true
		}
	}

	if tryWrite {
		m.writeIfNeeded()
	}
}

func (m *RecordMerger) writeIfNeeded() {
	// Don't actually write any records if there aren't any!
	//	(eg. when we've been triggered by a short circuit event rather than a record count event)
	if m.writableCounter > 0 {
		// We've got enough records to write the file now...
		writerInfo := WriterInfo{
			Timestamp:           (*m.aRecord).Start,
			NumRecords:          m.recordCounter,
			WritableRecords:     m.writableCounter,
			RecordMap:           m.recordMap,
			totalRecordsWritten: m.totalRecordsWritten,
		}

		m.recordMap = make(map[string]*data.LogToProcess)
		m.recordCounter = 0
		m.writableCounter = 0
		m.writerChannel <- &writerInfo
	}
}

func (m *RecordMerger) writerLoop() {
	printer := message.NewPrinter(message.MatchLanguage("en"))
	var pw *writer.ParquetWriter
	var fw source.ParquetFile
	var key string
	var fileRecordsWritten int64
	// Write out record maps as they come in...
	for info := range m.writerChannel {
		for pw == nil {
			failureCount := 0
			pw, fw, key = m.openParquetWriter()
			if pw == nil {
				failureCount++
				if failureCount > 25 {
					log.Fatal("Cannot open parquet writer - failed more than 25 times to do so - will quit!")
				}
				time.Sleep(200 * time.Millisecond)
			}
		}

		if util.TraceLoggingEnabled {
			util.PrintMemUsage(printer.Sprintf("Writing %d entries (Originally %d records, Total so far: %d) to parquet at: s3://%s/%s - Memory Stats: ", info.WritableRecords, info.NumRecords, info.totalRecordsWritten, m.writeToBucket, key))
		}

		m.writeFile(pw, &info.RecordMap, info.NumRecords, info.WritableRecords, info.Timestamp)
		fileRecordsWritten += info.WritableRecords

		if fileRecordsWritten > 10000000 { // More than 10m records (that'll come in at ~250MB), let's start a new file...
			if util.DebugLoggingEnabled {
				log.Printf("Closing Parquet File: s3://%s/%s, as it has over 10m records", m.writeToBucket, key)
			}
			m.closeParquetWriter(pw, fw)
			pw = nil
			fw = nil
			fileRecordsWritten = 0
		}
	}

	if pw != nil {
		log.Printf("Closing Parquet File: s3://%s/%s, as the writer is shutting down", m.writeToBucket, key)
		m.closeParquetWriter(pw, fw)
	}

	log.Println("Marking Writer loop as done...")
	m.workerWG.Done()
}

func (m *RecordMerger) closeParquetWriter(pw *writer.ParquetWriter, fw source.ParquetFile) {
	// Close the Writer...
	if err := pw.WriteStop(); err != nil {
		log.Println("Failed to stop the parquet writer with err", err.Error())
	}

	if err := fw.Close(); err != nil {
		log.Println("Error closing S3 file writer:", err.Error())
	}
}
func (m *RecordMerger) openParquetWriter() (*writer.ParquetWriter, source.ParquetFile, string) {
	// Build a key from the timestamp
	timestamp := m.aRecord.Start
	timeString := timestamp.Format("2006-01-02-03-04")
	timePathString := timestamp.Format("2006/01/02")
	uid, _ := uuid.NewV4()
	key := m.writeToPath + timePathString + "/" + "flowlogs-" + timeString + "-" + uid.String() + ".parquet"

	// Create an S3 Writer
	ctx := context.Background()
	uploaderOptions := make([]func(*s3manager.Uploader), 1)
	uploaderOptions[0] = s3manager.WithUploaderRequestOptions()
	fw, err := s3Parquet.NewS3FileWriter(ctx, m.writeToBucket, key, uploaderOptions)
	if err != nil {
		log.Println("Failed to load an S3 Writer with Error:", err.Error())
		return nil, nil, ""
	}

	// Create a Parquet Writer
	pw, err := writer.NewParquetWriter(fw, new(data.LogEntry), 4)
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	if err != nil {
		log.Println("Failed to create the parquet writer with error:", err.Error())
		return nil, nil, ""
	}

	return pw, fw, key
}
func (m *RecordMerger) writeFile(pw *writer.ParquetWriter, recordMap *map[string]*data.LogToProcess, recordCount int64, writableCount int64, timestamp time.Time) {
	rMap := *recordMap
	for k, v := range rMap {
		if err := pw.Write(v.Log); err != nil {
			log.Println("Encountered an error writing a record to the parquet file, will ignore it. Error:", err.Error(), ", Record:", k)
		}

		v.Done()
	}
}
