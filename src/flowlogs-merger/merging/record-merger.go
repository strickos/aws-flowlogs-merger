package merging

import (
	"context"
	awsUtil "flowlogs-merger/aws"
	"flowlogs-merger/data"
	"flowlogs-merger/util"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"golang.org/x/text/message"

	"github.com/satori/go.uuid"

	s3Parquet "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
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

	s3Client            *s3.S3
	athenaClient        *athena.Athena
	dbName              string
	tableName           string
	athenaResultsLoc    string
	partitioningEnabled bool

	maxRecordsPerFile int64
	compressionType   parquet.CompressionCodec
}

/*
MakeRecordMerger creates a RecordMerger worker and returns it.
*/
func MakeRecordMerger(forHour int, deadline time.Time, writeToBucket string, writeToPath string, records <-chan *data.LogToProcess, recordTracker *RecordTracker) (*RecordMerger, *sync.WaitGroup) {
	mergerer := RecordMerger{
		forHour:             forHour,
		deadline:            deadline,
		records:             records,
		writeToBucket:       writeToBucket,
		writeToPath:         writeToPath,
		writerChannel:       make(chan *WriterInfo),
		recordTracker:       recordTracker,
		s3Client:            awsUtil.NewS3Client(),
		athenaClient:        awsUtil.NewAthenaClient(),
		dbName:              util.GetEnvProp("GLUE_DATABASE_NAME", "flowlogs"),
		tableName:           util.GetEnvProp("GLUE_TABLE_NAME", "data"),
		athenaResultsLoc:    util.GetEnvProp("ATHENA_RESULTS_LOC", ""),
		partitioningEnabled: util.GetEnvProp("AUTO_PARTITIONING", "true") == "true",
	}

	mergerer.recordMap = make(map[string]*data.LogToProcess)
	mergerer.launchedWriterLoop = false
	recordsPerFile, err := strconv.Atoi(util.GetEnvProp("MAX_RECORDS_PER_FILE", "20000000"))
	if err == nil {
		mergerer.maxRecordsPerFile = int64(recordsPerFile)
	} else {
		log.Printf("Invalid Number of records per file specified! [Error: %s]", err.Error())
		mergerer.maxRecordsPerFile = 20000000 // Default: More than 20m records (that'll come in at ~512MB)
	}

	// Setup the Compression type to use
	compType := strings.ToUpper(util.GetEnvProp("COMPRESSION", "SNAPPY"))
	switch compType {
	case "SNAPPY":
		mergerer.compressionType = parquet.CompressionCodec_SNAPPY
	case "GZIP":
		mergerer.compressionType = parquet.CompressionCodec_GZIP
	case "GZ":
		mergerer.compressionType = parquet.CompressionCodec_GZIP
	case "LZO":
		mergerer.compressionType = parquet.CompressionCodec_LZO
	case "NONE":
		mergerer.compressionType = parquet.CompressionCodec_UNCOMPRESSED
	}

	if len(mergerer.athenaResultsLoc) == 0 {
		// Default to a qresults folder under the output path if not specified
		mergerer.athenaResultsLoc = "s3://" + mergerer.writeToBucket + "/" + writeToPath + "qresults/"
	}

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

		if fileRecordsWritten > m.maxRecordsPerFile {
			if util.DebugLoggingEnabled {
				log.Printf("Closing Parquet File: s3://%s/%s, as it has more than the max. allowed records", m.writeToBucket, key)
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
	prefix := m.writeToPath + timePathString + "/"
	m.addAthenaPartitionIfNeeded(prefix, timestamp)
	key := prefix + "flowlogs-" + timeString + "-" + uid.String() + ".parquet"

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
	pw.CompressionType = m.compressionType
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

func (m *RecordMerger) addAthenaPartitionIfNeeded(prefix string, timestamp time.Time) {
	if !m.partitioningEnabled {
		return
	}

	partitionFile := m.writeToPath + "partition-info/partition-" + timestamp.Format("2006-01-02") + ".txt"
	fileExists, err := m.s3FileExists(m.writeToBucket, partitionFile)
	if fileExists || err != nil {
		if err != nil {
			log.Printf("Failed to check for the presence of the partition file in s3, will ignore and assume that another lambda will be able to do this. Error: %s", err.Error())
		}
		return
	}

	monthString := timestamp.Format("2006-01")
	dayString := timestamp.Format("2006-01-02")
	queryString := fmt.Sprintf("ALTER TABLE %s ADD IF NOT EXISTS PARTITION (year=%d, month='%s', day='%s') LOCATION 's3://%s/%s'", m.tableName, timestamp.Year(), monthString, dayString, m.writeToBucket, prefix)
	var qParams athena.StartQueryExecutionInput
	qParams.SetQueryString(queryString)

	var ctxParam athena.QueryExecutionContext
	ctxParam.SetDatabase(m.dbName)
	qParams.SetQueryExecutionContext(&ctxParam)

	var qResConfig athena.ResultConfiguration
	qResConfig.SetOutputLocation(m.athenaResultsLoc)
	qParams.SetResultConfiguration(&qResConfig)

	startRes, err := m.athenaClient.StartQueryExecution(&qParams)
	if err != nil {
		log.Printf("Failure attempting to add partition \"%s\" to the athena table. Error: %s", prefix, err.Error())
		// NB: As the partition file won't be written, this'll be attempted by another lambda in this partition
		return
	}

	var params athena.GetQueryExecutionInput
	params.SetQueryExecutionId(*startRes.QueryExecutionId)

	var qrop *athena.GetQueryExecutionOutput
	duration := time.Duration(500) * time.Millisecond // Wait 1/2 sec between checks (query typically takes about 400ms)

	attemptCounter := 0
	for {
		attemptCounter++
		if attemptCounter > 20 { // ~ 10s
			log.Println("Giving up waiting for ADD PARTITION query to complete - have been waiting for too long without any update")
			return
		}

		qrop, err = m.athenaClient.GetQueryExecution(&params)
		if err != nil {
			log.Printf("Failure querying Athena to check on the progress of the add partition query. Error: %s", err.Error())
		}

		if *qrop.QueryExecution.Status.State != "RUNNING" {
			break
		}

		time.Sleep(duration)
	}

	if *qrop.QueryExecution.Status.State == "SUCCEEDED" { // Write Partition file
		putParams := &s3.PutObjectInput{
			Bucket: aws.String(m.writeToBucket),
			Key:    aws.String(partitionFile),
			Body:   strings.NewReader(queryString),
		}

		_, err := m.s3Client.PutObject(putParams)
		if err != nil {
			log.Printf("ADD PARTITION Query succeeded, but failed to write the partition file to S3 - this process will be repeated by another lambda. Error: %s", err.Error())
		} else {
			log.Printf("Created Glue Table Partition: year=%d, month=%s, day=%s @ s3://%s/%s", timestamp.Year(), monthString, dayString, m.writeToBucket, prefix)
		}
	} else { // Failed, ignore and hope next lambda in this partition will succeed
		log.Printf("ADD PARTITION Query did not complete successfully - it completed with state: %s", *qrop.QueryExecution.Status.State)
	}
}

func (m *RecordMerger) s3FileExists(bucket string, key string) (bool, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	_, err := m.s3Client.HeadObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				log.Fatalf("Specified output bucket doesn't exist - cannot proceed! [Got: %s]", bucket)
			case s3.ErrCodeNoSuchKey:
				// Key doesn't exist
				return false, nil
			case "NotFound":
				// Key doesn't exist
				return false, nil
			default:
				// Some other failure
				return false, err
			}
		} else {
			return false, err
		}
	}

	// No Error means the file exists...
	return true, nil
}
