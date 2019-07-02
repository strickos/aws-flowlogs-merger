package merging

import (
	"context"
	"encoding/json"
	awsUtil "flowlogs-merger/aws"
	"flowlogs-merger/data"
	"flowlogs-merger/mapping"
	"flowlogs-merger/util"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func HandleMergeRequest(ctx context.Context, event events.SQSEvent) (string, error) {
	realDeadline, _ := ctx.Deadline()
	minusOneMinute, _ := time.ParseDuration("-5m")
	deadLine := realDeadline.Add(minusOneMinute) // Subtract 5 minutes from deadline to be safe
	log.Println("Deadline:", realDeadline, ", Stopping at:", deadLine)
	records := event.Records
	if len(records) == 0 {
		return "Nothing to merge", nil
	}
	deadlineChannel := time.After(time.Until(deadLine))

	data1 := mapping.MergeInvokeEvent{}
	util.FromJSON(&records[0].Body, &data1)

	recordTracker := makeRecordTracker()

	// Start the Record Merge Worker
	recordChannel, workerWG := startRecordMergeWorker(data1.ForHour, deadLine, recordTracker)

	// Start the File Processors
	filesToProcess, largeFilesToProcess, returnToSQSChannel, fileProcessorsWG := startFileProcessors(recordChannel, recordTracker)

	filesWG := sync.WaitGroup{}
	var filesInProgress int32
	var dontDoAnyMore = false

	filesToReturnToSQS := make([]data.FileToProcessInfo, 0)
	go grabFilesToReturnToSQS(returnToSQSChannel, &filesToReturnToSQS)

	for _, record := range records {
		event := mapping.MergeInvokeEvent{}
		util.FromJSON(&record.Body, &event)

		if event.ForHour != data1.ForHour {
			// If the record is for a different hour to this one, then return it to the SQS queue
			returnFilesToSQS(&event.Records, event.ForHour)
		} else {
			for _, file := range event.Records {
				if dontDoAnyMore {
					// Add to list of files to be put back to SQS
					filesToReturnToSQS = append(filesToReturnToSQS, file)
				} else {
					var fileQueue chan *data.FileToProcessInfo
					if file.Size > 5242880 { // > 5MB
						fileQueue = largeFilesToProcess
					} else {
						fileQueue = filesToProcess
					}

					wg := sync.WaitGroup{} // WG for the file
					wg.Add(1)
					f2 := data.MakeFileToProcessInfo(file.Bucket, file.Key, file.Size, &wg)

					if file.Key[0] == '!' {
						// We just need to mark this file as done and move on!
						f2.Key = file.Key[1:len(file.Key)]
						_, err := recordTracker.MarkFileAsDone(f2)
						if err != nil {
							filesToReturnToSQS = append(filesToReturnToSQS, file)
						}
						continue
					}

					trackingState := recordTracker.MarkFileAsProcessing(f2)
					if trackingState == -1 {
						// We should retry this file later...
						filesToReturnToSQS = append(filesToReturnToSQS, file)
					} else if trackingState == 1 {
						select {
						case fileQueue <- f2:
							filesWG.Add(1)
							// fileIncrement := int32(math.Ceil(float64(file.Size / 2097152))) // 1 reader per 2MB
							atomic.AddInt32(&filesInProgress, 1)
							go awaitFileToBeProcessed(&wg, &filesWG, &filesInProgress, 1, f2, recordTracker, &filesToReturnToSQS)
						case <-deadlineChannel:
							// Deadline Looming, let's add this file to the list of files to do later ...
							filesToReturnToSQS = append(filesToReturnToSQS, file)
						}
					} else {
						log.Printf("Skipping file [s3://%s/%s] as it has already been processed", f2.Bucket, f2.Key)
					}

					if time.Now().After(deadLine) {
						log.Println("Stopping processing of Files due to looming deadline - will send remaining files as a new SQS Message for future work")
						dontDoAnyMore = true
					}
				}

			}
		}
	}

	// Wait for the Record Merger to finish
	log.Println("All files pushed into the files to process channel, will wait for the files to be processed...")
	close(filesToProcess)
	close(largeFilesToProcess)
	fileProcessorsWG.Wait()
	log.Println("All file readers closed, closing record channel...")
	close(recordChannel)
	close(returnToSQSChannel)

	log.Println("All files scanned, will wait for all records from the files to be processed...")
	filesWG.Wait()

	log.Println("Waiting for the parquet writer to finish writing...")
	workerWG.Wait()

	// Send the files to return to a new SQS message to be processed
	if len(filesToReturnToSQS) > 0 {
		go returnFilesToSQS(&filesToReturnToSQS, data1.ForHour)
	}

	log.Println("All records processed, exiting...")
	return "", nil
}

func awaitFileToBeProcessed(messageWG *sync.WaitGroup, filesWG *sync.WaitGroup, filesInProgress *int32, fileIncrement int32, file *data.FileToProcessInfo, recordTracker *RecordTracker, filesToReturnToSQS *[]data.FileToProcessInfo) {
	messageWG.Wait()
	_, err := recordTracker.MarkFileAsDone(file)
	if err != nil {
		// An error occurred marking this file as done
		file.Key = "!" + file.Key
		*filesToReturnToSQS = append(*filesToReturnToSQS, *file)
	}
	atomic.AddInt32(filesInProgress, -1*fileIncrement)
	filesWG.Done()
}

func grabFilesToReturnToSQS(returnToSQSChannel chan *data.FileToProcessInfo, filesToReturnToSQS *[]data.FileToProcessInfo) {
	for file := range returnToSQSChannel {
		*filesToReturnToSQS = append(*filesToReturnToSQS, *file)
	}
}

func startFileProcessors(recordChannel chan *data.LogToProcess, recordTracker *RecordTracker) (chan *data.FileToProcessInfo, chan *data.FileToProcessInfo, chan *data.FileToProcessInfo, *sync.WaitGroup) {
	fileProcessorsWG := sync.WaitGroup{}

	// Launch Standard Workers
	filesToProcess := make(chan *data.FileToProcessInfo, 10)
	largeFilesToProcess := make(chan *data.FileToProcessInfo, 1)
	returnToSQSChannel := make(chan *data.FileToProcessInfo)

	numReadersPref := util.GetEnvProp("NUM_FILE_READERS", "4")
	numFileReaders, err := strconv.Atoi(numReadersPref)
	if err != nil {
		log.Fatalf("Invalid number of File readers specified [%s]", numReadersPref)
	}
	fileWorkers := make([]*FileProcessor, numFileReaders)
	for workerNum := 0; workerNum < numFileReaders; workerNum++ {
		fileWorkers[workerNum] = MakeFileProcessor(util.Region(), filesToProcess, recordChannel, returnToSQSChannel, &fileProcessorsWG, recordTracker)
		go fileWorkers[workerNum].Run()
	}

	// Launch Large File Workers
	numLargeFileReadersPref := util.GetEnvProp("NUM_LARGE_FILE_READERS", "1")
	numLargeFileReaders, err := strconv.Atoi(numLargeFileReadersPref)
	if err != nil {
		log.Fatalf("Invalid number of Large File readers specified [%s]", numLargeFileReadersPref)
	}

	largeFileWorkers := make([]*FileProcessor, numLargeFileReaders)
	for workerNum := 0; workerNum < numLargeFileReaders; workerNum++ {
		largeFileWorkers[workerNum] = MakeFileProcessor(util.Region(), largeFilesToProcess, recordChannel, returnToSQSChannel, &fileProcessorsWG, recordTracker)
		go largeFileWorkers[workerNum].Run()
	}
	return filesToProcess, largeFilesToProcess, returnToSQSChannel, &fileProcessorsWG
}

func startRecordMergeWorker(forHour int, deadline time.Time, recordTracker *RecordTracker) (chan *data.LogToProcess, *sync.WaitGroup) {
	outputBucket := util.GetEnvProp("OUTPUT_BUCKET", "adams-got-data")
	outputPath := util.GetEnvProp("OUTPUT_PATH", "output/")

	recordChannel := make(chan *data.LogToProcess, 50000)
	recordMerger, workerWG := MakeRecordMerger(forHour, deadline, outputBucket, outputPath, recordChannel, recordTracker)
	go recordMerger.Run()

	return recordChannel, workerWG
}

func returnFilesToSQS(filesToReturn *[]data.FileToProcessInfo, forHour int) {
	event := mapping.MergeInvokeEvent{
		Action:  "merge",
		ForHour: forHour,
	}

	builder := strings.Builder{}
	for _, file := range *filesToReturn {
		builder.WriteString(fmt.Sprintf("\n - s3://%s/%s", file.Bucket, file.Key))
		event.Records = append(event.Records, file)
	}

	msg, err := json.Marshal(event)
	if err != nil {
		log.Fatalf("Failed to build JSON for Merge Message to SQS with %d records. Size: %d:", filesToReturn, len(msg))
	} else {
		log.Printf("Returning %d Files to SQS to be processed later: %s", len(*filesToReturn), builder.String())
	}

	sqsClient := awsUtil.NewSqsClient()
	sqsQueueName := util.GetEnvProp("MERGE_QUEUE_NAME", "flowlogs-merge")
	queueURL, err := awsUtil.GetQueueURL(sqsQueueName, sqsClient)
	if err != nil {
		log.Fatalf("Failed to find the URL of the Merge SQS Queue with Error: %s", err.Error())
	}

	params := &sqs.SendMessageInput{
		MessageBody: aws.String(string(msg)),
		QueueUrl:    aws.String(queueURL),
	}

	_, sendErr := sqsClient.SendMessage(params)
	if sendErr != nil {
		log.Fatalf("Failure returning un-processed files to SQS. Error: %s", err.Error())
	}
}
