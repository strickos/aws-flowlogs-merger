package mapping

import (
	"encoding/json"
	awsUtil "flowlogs-merger/aws"
	"flowlogs-merger/data"
	"flowlogs-merger/util"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

/*
MergeInvokeEvent describes a file to be processed by a merger
*/
type MergeInvokeEvent struct {
	Action    string                   `json:"action"`
	Records   []data.FileToProcessInfo `json:"records"`
	ForHour   int                      `json:"forHour"`
	ForMinute int                      `json:"forMinute"`
}

/*
FileCollectorForMerge is a worker go routine that will collect files for a particular hour.
*/
type FileCollectorForMerge struct {
	sqsClient *sqs.SQS
	forHour   int
	forMinute int
	channel   chan *data.FileToProcessInfo
	queueURL  string
}

/*
MakeFileCollectorForMerge creates a new Collector for Merge worker and returns it.
*/
func MakeFileCollectorForMerge(forHour int, forMinute int, queueURL string, channel chan *data.FileToProcessInfo) *FileCollectorForMerge {
	collector := FileCollectorForMerge{
		sqsClient: awsUtil.NewSqsClient(),
		forHour:   forHour,
		forMinute: forMinute,
		queueURL:  queueURL,
		channel:   channel,
	}
	return &collector
}

/*
Run starts the FileProcessor loop, reading files off the channel
*/
func (fp *FileCollectorForMerge) Run() {
	mergeEvent := MergeInvokeEvent{Action: "merge", ForHour: fp.forHour, ForMinute: fp.forMinute}
	var recordCounter int64
	var sizeEstimate int64
	for file := range fp.channel {
		file.TimestampVal = file.Timestamp.Unix()
		if recordCounter == 0 {
			mergeEvent.Records = make([]data.FileToProcessInfo, 1)
			mergeEvent.Records[0] = *file
		} else {
			mergeEvent.Records = append(mergeEvent.Records, *file)
		}

		jsonStringForEstimate := util.ToJSON(*file)
		sizeEstimate += int64(len(*jsonStringForEstimate) + 4)

		if file.UncompressedSize > 0 {
			recordCounter += file.UncompressedSize / 113 // 113 is the size of a large record (large ENI ID, long IP addresses)
		} else {
			recordCounter += file.Size / 14 // a terrible rudimentary estimate
		}

		if sizeEstimate > 204800 || recordCounter > 60000000 { // > ~200kb SQS Message or 60m records
			go fp.invokeMerge(mergeEvent, recordCounter, sizeEstimate)

			mergeEvent = MergeInvokeEvent{Action: "merge", ForHour: fp.forHour}
			recordCounter = 0
			sizeEstimate = 0
		}
	}

	if util.DebugLoggingEnabled {
		if recordCounter > 0 {
			log.Printf("File Collector for Merge is closing, will invoke final file now with the %d remaining records", recordCounter)
		} else {
			log.Printf("File Collector for Merge is closing")
		}
	}

	if recordCounter > 0 {
		fp.invokeMerge(mergeEvent, recordCounter, sizeEstimate)
	}
}

func (fp *FileCollectorForMerge) invokeMerge(mergeEvent MergeInvokeEvent, recordCounter int64, sizeEstimate int64) {
	msg, err := json.Marshal(mergeEvent)
	if err != nil {
		log.Printf("Failed to build JSON for Merge Message to SQS with %d records. Size: %d, Estimate: %d. Memory Stats: ", recordCounter, len(msg), sizeEstimate)
	} else {
		// log.Printf("Merge Message Records: %d", len(msg.))
		log.Println("Merge Event Records:", len(mergeEvent.Records))
	}
	if util.DebugLoggingEnabled {
		util.PrintMemUsage(fmt.Sprintf("Sending Merge Message to SQS with %d records. Size: %d, Estimate: %d. Memory Stats: ", recordCounter, len(msg), sizeEstimate))
	}
	params := &sqs.SendMessageInput{
		MessageBody: aws.String(string(msg)),
		QueueUrl:    aws.String(fp.queueURL),
	}

	_, sendErr := fp.sqsClient.SendMessage(params)
	if sendErr != nil {
		log.Printf("Failure sending SQS Message. Error: %s", err.Error())
	}
}
