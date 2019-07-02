package mapping

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	awsUtil "flowlogs-merger/aws"
	"flowlogs-merger/data"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

/*
FileTimeRangeProcessor is a worker go routine that will process files out of the "filesToProcess" channel.
*/
type FileTimeRangeProcessor struct {
	s3Client       *s3.S3
	wg             *sync.WaitGroup
	filesToProcess chan *data.FileToProcessInfo
	hourlyChannels []chan *data.FileToProcessInfo
}

/*
MakeFileTimeRangeProcessor creates a new FileTimeRangeProcessor and returns it.
*/
func MakeFileTimeRangeProcessor(filesToProcess chan *data.FileToProcessInfo, hourlyChannels *[]chan *data.FileToProcessInfo, wg *sync.WaitGroup) *FileTimeRangeProcessor {
	processor := FileTimeRangeProcessor{
		s3Client:       awsUtil.NewS3Client(),
		wg:             wg,
		filesToProcess: filesToProcess,
		hourlyChannels: *hourlyChannels,
	}
	return &processor
}

/*
Run starts the FileProcessor loop, reading files off the channel
*/
func (fp *FileTimeRangeProcessor) Run() {
	for file := range fp.filesToProcess {
		// if util.DebugLoggingEnabled {
		// 	log.Printf("Determining Hour of file: s3://%s/%s", file.Bucket, file.Key)
		// }
		if len(file.Key) == 0 {
			tmp, _ := json.Marshal(*file)
			log.Printf("Oh dear - no file specified in the SQS Payload: %s", string(tmp))
			continue
		}

		fp.processFile(file)
		file.Done()
	}
}

func (fp *FileTimeRangeProcessor) processFile(file *data.FileToProcessInfo) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(file.Bucket),
		Key:    aws.String(file.Key),
	}

	// Only grab enough to read the first few lines...
	if file.Size > 2048 {
		params.Range = aws.String("bytes=0-2048") // Just grab first 2kb
	}

	// Get the Object, Reading as GZ
	obj, err := fp.s3Client.GetObject(params)
	gz, err := gzip.NewReader(obj.Body)
	if err != nil {
		log.Printf("Failed to load the GZip Reader [File: s3://%s/%s], with Error: %s", file.Bucket, file.Key, err.Error())
	}

	defer obj.Body.Close()
	defer gz.Close()

	var logData *data.LogEntry
	scanner := bufio.NewScanner(gz)
	if scanner.Scan() { // Skip first line - it's the header....
		for scanner.Scan() {
			line := scanner.Text()
			record, err := data.ParseLogEntry(&line)
			if err != nil {
				log.Printf("%s [%d] in File [s3://%s/%s]", err.Error(), 2, file.Bucket, file.Key)
			} else {
				logData = record
				break
			}
		}
	}

	if logData != nil {
		file.Timestamp = logData.Start
		collectionChannelNumber := (logData.Start.Hour() * 60) + logData.Start.Minute()
		fp.hourlyChannels[collectionChannelNumber] <- file
	} else {
		// todo: put this into an exception queue to be processed manually
		log.Printf("Oh dear - this file doesn't appear to be in a valid FlowLogs format, will skip this file: [s3://%s/%s]", file.Bucket, file.Key)
	}
}
