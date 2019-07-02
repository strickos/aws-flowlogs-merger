package merging

import (
	"bufio"
	"compress/gzip"
	"flowlogs-merger/data"
	"flowlogs-merger/util"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"golang.org/x/text/message"
)

/*
FileProcessor is a worker go routine that will process files out of the "filesToProcess" channel.
*/
type FileProcessor struct {
	s3Client           *s3.S3
	filesToProcess     <-chan *data.FileToProcessInfo
	recordChannel      chan<- *data.LogToProcess
	returnToSQSChannel chan<- *data.FileToProcessInfo
	fileProcessorsWG   *sync.WaitGroup
	recordTracker      *RecordTracker
}

/*
MakeFileProcessor creates a new FileProcessor and returns it.
*/
func MakeFileProcessor(region string, filesToProcess <-chan *data.FileToProcessInfo, recordChannel chan<- *data.LogToProcess, returnToSQSChannel chan<- *data.FileToProcessInfo, fileProcessorsWG *sync.WaitGroup, recordTracker *RecordTracker) *FileProcessor {
	processor := FileProcessor{
		s3Client:           s3.New(session.New(), aws.NewConfig().WithRegion(region)),
		filesToProcess:     filesToProcess,
		recordChannel:      recordChannel,
		returnToSQSChannel: returnToSQSChannel,
		fileProcessorsWG:   fileProcessorsWG,
		recordTracker:      recordTracker,
	}
	fileProcessorsWG.Add(1)
	return &processor
}

/*
Run starts the FileProcessor loop, reading files off the channel
*/
func (fp *FileProcessor) Run() {
	printer := message.NewPrinter(message.MatchLanguage("en"))

	for file := range fp.filesToProcess {
		if util.TraceLoggingEnabled {
			log.Printf("Reading File: s3://%s/%s", file.Bucket, file.Key)
		}

		// Process the File...
		fp.processFile(file, printer)

		if util.TraceLoggingEnabled {
			log.Printf("Finished Reading File: s3://%s/%s", file.Bucket, file.Key)
		}

		// Mark this FileToProcess as done!
		file.Done()
	}

	log.Println("File Processing Worker is closing...")
	fp.fileProcessorsWG.Done()
}

func (fp *FileProcessor) processFile(file *data.FileToProcessInfo, printer *message.Printer) {
	var lineCounter int64 = 1 // Start at 1 because we're skipping the first line (header)

	// Open S3 File
	pr, pw := io.Pipe()
	go fp.openFile(file, pr, pw, &lineCounter)

	// Read as GZ File
	gz, err := gzip.NewReader(pr)
	defer gz.Close()
	if err != nil {
		log.Printf("Failed to Read the GZipped File [s3://%s/%s] with Error: %s", file.Bucket, file.Key, err.Error())
		// Will put the file into the return to SQS channel to be processed later
		fp.recordTracker.MarkFileAsFailed(file)
		fp.returnToSQSChannel <- file
		return
	}

	// Start Scanning through the file, one line at a time...
	scanner := bufio.NewScanner(gz)
	if scanner.Scan() { // Skip first line - it's the header....
		fileHash := hash32(fmt.Sprintf("s3://%s/%s", file.Bucket, file.Key))
		for scanner.Scan() {
			atomic.AddInt64(&lineCounter, 1)

			line := scanner.Text()
			record, err := data.ParseLogEntry(&line)
			if err != nil {
				log.Printf("%s [%d] in File [s3://%s/%s]", err.Error(), lineCounter, file.Bucket, file.Key)
			} else {
				record.OriginHash = fileHash
				entry := data.MakeLogToProcess(record, file)
				fp.recordChannel <- entry
			}
		}
	}
}

func hash32(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func (fp *FileProcessor) openFile(file *data.FileToProcessInfo, pr *io.PipeReader, pw *io.PipeWriter, lineCounter *int64) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(file.Bucket),
		Key:    aws.String(file.Key),
	}

	obj, err := fp.s3Client.GetObject(params)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeNoSuchBucket:
				log.Printf("The bucket for this file [s3://%s/%s] does not exist - will skip this file", file.Bucket, file.Key)
				pw.Close()
				return
			case s3.ErrCodeNoSuchKey:
				log.Printf("The specified key [s3://%s/%s] does not exist - will skip this file", file.Bucket, file.Key)
				pw.Close()
				return
			}
		} // else not an AWS error...

		log.Printf("Failed to Retrieve File [s3://%s/%s] from S3 - will return it to the queue of files to process. Error Message: %s", file.Bucket, file.Key, err.Error())
		// Put this file into the "return to SQS" channel - we'll return it to SQS and attempt to process it later
		fp.recordTracker.MarkFileAsFailed(file)
		fp.returnToSQSChannel <- file

		// Close the writer, there's nothing to do with this file...
		pw.Close()
		return
	}

	io.Copy(pw, obj.Body)
	pw.Close()
}
