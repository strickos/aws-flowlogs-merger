package mapping

import (
	"context"
	awsUtil "flowlogs-merger/aws"
	"flowlogs-merger/data"
	"flowlogs-merger/util"
	"log"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

/*
HandleMapRequest handles a new Lambda Invoke with the "map" action
This action will map as many files as it can off the SQS queue into groups of "merge" invokes for groups of files by hour
*/
func HandleMapRequest(ctx context.Context, event events.CloudWatchEvent) (string, error) {
	region := util.Region()

	deadLine, _ := ctx.Deadline()
	minusOneMinute, _ := time.ParseDuration("-5m")
	deadLine = deadLine.Add(minusOneMinute) // Subtract 5 mins from deadline to be safe

	// Start the Record Mergers
	recordChannels, _, recordsWG := startRecordCollectors()

	// Start the FileProcessor Workers
	filesToProcess, _, numFileReaders := startFileProcessors(region, recordChannels, recordsWG)

	// Start Pulling records from the SQS Queue
	sqsClient := awsUtil.NewSqsClient()
	queueName := util.GetEnvProp("MAP_QUEUE_NAME", "flowlogs-raw")
	queueURL, err := awsUtil.GetQueueURL(queueName, sqsClient)
	if err != nil {
		log.Fatalf("Failed to retrieve the Queue URL for queue [%s] with Error: %s", queueName, err.Error())
	}

	sqsParams := &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(10),
		QueueUrl:            aws.String(queueURL),
	}

	doneWG := sync.WaitGroup{}
	var inFlightMessages int32
	for time.Until(deadLine).Minutes() > 0 {
		if inFlightMessages > 4*numFileReaders { // We only want to keep enough messages in the queue so that there's always 3 messages ready for the reader
			time.Sleep(100 * time.Millisecond)
		}

		msgOut, sqsErr := sqsClient.ReceiveMessage(sqsParams)
		if sqsErr != nil {
			log.Printf("Failure Pulling Messages from SQS: %s", sqsErr.Error())
		} else {
			if len(msgOut.Messages) > 0 {
				if util.DebugLoggingEnabled {
					log.Printf("Processing SQS Message from RAW Queue with %d files to process.", len(msgOut.Messages))
				}

				for _, msg := range msgOut.Messages {
					// msg.Body
					var s3Event events.S3Event
					err := util.FromJSON(msg.Body, &s3Event)
					if err != nil {
						log.Printf("Failed to unmarshal the body of SQS Message [Message ID: %s] with Error: %s", *msg.MessageId, err.Error())
					} else {
						wg := sync.WaitGroup{}
						for _, record := range s3Event.Records {
							info := data.MakeFileToProcessInfo(record.S3.Bucket.Name, record.S3.Object.Key, record.S3.Object.Size, &wg)
							wg.Add(1)
							filesToProcess <- info
						}

						doneWG.Add(1)
						atomic.AddInt32(&inFlightMessages, 1)
						go awaitAllRecordsInMessageProcessed(msg, queueURL, &wg, sqsClient, &doneWG, &inFlightMessages)
					}
				}
			}
		}
	}

	// Close the Files to Process Channel (We've added all the files we want to process to it)
	log.Println("Closing FilesToProcess Channel - all files for this run have been added to it")
	close(filesToProcess)

	log.Println("Waiting for File Processors to finish...")
	doneWG.Wait()

	log.Println("Closing All Record Channels")
	for _, channel := range *recordChannels {
		if channel != nil {
			close(channel)
		}
	}

	log.Println("Waiting for Record Collector Workers to finish processing their records...")
	(*recordsWG).Wait()

	log.Println("Done!")
	return "ok", nil
}

func startFileProcessors(region string, recordChannels *[]chan *data.FileToProcessInfo, recordsWG *sync.WaitGroup) (chan *data.FileToProcessInfo, *[]*FileTimeRangeProcessor, int32) {
	// Setup File Reader Workers
	filesToProcess := make(chan *data.FileToProcessInfo)
	numReadersPref := util.GetEnvProp("NUM_FILE_READERS", "5")
	numFileReaders, err := strconv.Atoi(numReadersPref)
	if err != nil {
		log.Fatalf("Invalid number of File readers specified [%s]", numReadersPref)
	}

	fileWorkers := make([]*FileTimeRangeProcessor, numFileReaders)
	for workerNum := 0; workerNum < numFileReaders; workerNum++ {
		fileWorkers[workerNum] = MakeFileTimeRangeProcessor(filesToProcess, recordChannels, recordsWG)
		go fileWorkers[workerNum].Run()
	}
	return filesToProcess, &fileWorkers, int32(numFileReaders)
}

func startRecordCollectors() (*[]chan *data.FileToProcessInfo, *[]*FileCollectorForMerge, *sync.WaitGroup) {
	sqsQueueName := util.GetEnvProp("MERGE_QUEUE_NAME", "flowlogs-merge")
	queueURL, err := awsUtil.GetQueueURL(sqsQueueName, awsUtil.NewSqsClient())
	if err != nil {
		log.Fatalf("Failed to find the URL of the Merge SQS Queue with Error: %s", err.Error())
	}
	recordChannels := make([]chan *data.FileToProcessInfo, int(math.Ceil(24*60/5)))
	recordCollectors := make([]*FileCollectorForMerge, int(math.Ceil(24*60/5)))
	recordsWG := sync.WaitGroup{}

	for h := 0; h < 24; h++ {
		for m := 0; m < 60; m++ {
			i := int32(math.Floor(float64((h*60)+m) / float64(5)))
			recordChannels[i] = make(chan *data.FileToProcessInfo)
			recordCollectors[i] = MakeFileCollectorForMerge(h, m, queueURL, recordChannels[i], &recordsWG)
			go recordCollectors[i].Run()
		}
	}

	return &recordChannels, &recordCollectors, &recordsWG
}

func awaitAllRecordsInMessageProcessed(msg *sqs.Message, queueURL string, wg *sync.WaitGroup, sqsClient *sqs.SQS, doneWG *sync.WaitGroup, inFlightMessages *int32) {
	wg.Wait() // Wait for all the records in the message to be processed before deleting the message from the queue

	if util.DebugLoggingEnabled {
		log.Printf("Deleting Fully Processed SQS Message ID: %s, with ReceiptHandle:%s", *msg.MessageId, *msg.ReceiptHandle)
	}

	params := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: msg.ReceiptHandle,
	}

	_, err := sqsClient.DeleteMessage(params)
	if err != nil {
		println("Failed to Delete Message from the queue:", msg.MessageId)
	}

	atomic.AddInt32(inFlightMessages, -1)
	doneWG.Done()
}
