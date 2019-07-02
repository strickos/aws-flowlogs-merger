package aws

import (
	"flowlogs-merger/util"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

/*
GetQueueURL returns the URL of the specified queue
*/
func GetQueueURL(queueName string, sqsClient *sqs.SQS) (string, error) {
	if util.DebugLoggingEnabled {
		println("Getting URL of Queue:", queueName)
	}

	qParams := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	}
	qInfo, qErr := sqsClient.GetQueueUrl(qParams)
	if qErr != nil {
		return "", qErr
	}

	return *qInfo.QueueUrl, nil
}
