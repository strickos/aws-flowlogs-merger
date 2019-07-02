package aws

import (
	"flowlogs-merger/util"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
)

/*
NewSqsClient builds and returns a new SQS Client
*/
func NewSqsClient() *sqs.SQS {
	return sqs.New(session.New(), aws.NewConfig().WithRegion(util.Region()))
}

/*
NewS3Client builds and returns a new S3 Client
*/
func NewS3Client() *s3.S3 {
	return s3.New(session.New(), aws.NewConfig().WithRegion(util.Region()))
}

/*
NewDDBClient builds and returns a new DynamoDB Client
*/
func NewDDBClient() *dynamodb.DynamoDB {
	return dynamodb.New(session.New(), aws.NewConfig().WithRegion(util.Region()))
}
