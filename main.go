package main

import (
	"context"
	"flowlogs-merger/mapping"
	"flowlogs-merger/merging"
	"flowlogs-merger/util"
	"log"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"

	"github.com/aws/aws-lambda-go/lambda"
)

func main() {
	log.SetOutput(os.Stdout) // Log to std out (not default std err)

	// The action to perform is controlled by an environment variable
	// 2 supported actions:
	//	- map => Will read off the raw flowlogs SQS queue and map files into hourly buckets, sending batches to the merge queue
	// 	- merge => Will read batches off the merge queue, combining files in batch into one or more snappy-compressed parquet files (upto ~500MB)
	action := util.GetEnvProp("ACTION", "test")
	switch action {
	case "map":
		lambda.Start(mapping.HandleMapRequest)
	case "merge":
		lambda.Start(merging.HandleMergeRequest)
	case "test":
		testMerge()
	default:
		panic("Invalid Action specified!")
	}
}

// testRun is used when runing the main directly (outside of lambda)
func testMerge() {
	os.Setenv("USE_REGION", "us-east-1")
	os.Setenv("DEBUG_LOGGING", "true")
	jsonData := "{\"action\":\"merge\",\"records\":[{         \"bucket\": \"adams-got-data\",         \"key\": \"AWSLogs/034496339141/vpcflowlogs/us-east-1/2019/06/05/034496339141_vpcflowlogs_us-east-1_fl-093a2bd1f31cac72f_20190605T0835Z_8246457e.log.gz\",         \"size\": 35552739,         \"timestamp\": 1559717343 }] }"
	event := events.SQSEvent{}
	event.Records = append(event.Records, events.SQSMessage{Body: jsonData})
	deadline := time.Unix(time.Now().Unix()+900, 0)
	ctx, cncl := context.WithDeadline(context.Background(), deadline)
	merging.HandleMergeRequest(ctx, event)
	cncl()
}
