package merging

import (
	awsUtil "flowlogs-merger/aws"
	"flowlogs-merger/data"
	"flowlogs-merger/util"
	"log"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type RecordTracker struct {
	dynamoClient   *dynamodb.DynamoDB
	tableName      string
	expiryInterval time.Duration
	sixteenMinutes time.Duration
}

type TrackRecord struct {
	ID        string    `json:"id"`
	State     int       `json:"state"`
	StartTime time.Time `json:"start"`
	Expiry    time.Time `json:"expiry"`
}

func makeRecordTracker() *RecordTracker {
	tracker := RecordTracker{}
	tracker.dynamoClient = awsUtil.NewDDBClient()
	tracker.tableName = util.GetEnvProp("TRACKING_TABLE", "flowlogs-tracking")
	tracker.expiryInterval, _ = time.ParseDuration("48h")
	tracker.sixteenMinutes, _ = time.ParseDuration("-16m")
	return &tracker
}

func (t *RecordTracker) GetRecord(file *data.FileToProcessInfo) (*TrackRecord, error) {
	av := make(map[string]*dynamodb.AttributeValue)
	av["id"] = &dynamodb.AttributeValue{
		S: aws.String(file.Bucket + ":" + file.Key),
	}

	lookupParams := &dynamodb.GetItemInput{
		TableName:      aws.String(t.tableName),
		ConsistentRead: aws.Bool(true),
		Key:            av,
	}

	res, err := t.dynamoClient.GetItem(lookupParams)
	if err != nil {
		return nil, err
	}

	dbRecord := TrackRecord{}
	if res.Item != nil {
		err = dynamodbattribute.UnmarshalMap(res.Item, &dbRecord)
		if err != nil {
			return nil, err
		}
	}

	return &dbRecord, nil
}

func (t *RecordTracker) createRecord(file *data.FileToProcessInfo) (bool, error) {
	record := TrackRecord{
		ID:        file.Bucket + ":" + file.Key,
		State:     0,
		StartTime: time.Now(),
		Expiry:    time.Now().Add(t.expiryInterval),
	}
	av, err := dynamodbattribute.MarshalMap(record)
	if err != nil {
		return false, err
	}

	params := &dynamodb.PutItemInput{
		TableName:           aws.String(t.tableName),
		ConditionExpression: aws.String("attribute_not_exists(id)"),
		Item:                av,
	}

	_, err = t.dynamoClient.PutItem(params)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				// Failed condition check - not an error per say...
				return false, nil
			}
		}

		return false, err
	}

	// Item was created successfully
	return true, nil
}

func (t *RecordTracker) setState(state int, expectedState int, file *data.FileToProcessInfo, initTimes bool) (bool, error) {
	params := &dynamodb.UpdateItemInput{
		TableName: aws.String(t.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(file.Bucket + ":" + file.Key),
			},
		},
		UpdateExpression: aws.String("SET #S = :s"),
		ExpressionAttributeNames: map[string]*string{
			"#S": aws.String("state"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":s": {
				N: aws.String(strconv.Itoa(state)),
			},
			":x": {
				N: aws.String(strconv.Itoa(expectedState)),
			},
		},
		ConditionExpression: aws.String("#S = :x"),
	}

	if initTimes {
		params.UpdateExpression = aws.String("SET #S = :s, #ST = :st, #EX = :ex")
		params.ExpressionAttributeNames["#ST"] = aws.String("start")
		params.ExpressionAttributeNames["#EX"] = aws.String("expiry")
		params.ExpressionAttributeValues[":st"] = &dynamodb.AttributeValue{S: aws.String(time.Now().Format(time.RFC3339Nano))}
		params.ExpressionAttributeValues[":ex"] = &dynamodb.AttributeValue{S: aws.String(time.Now().Add(t.expiryInterval).Format(time.RFC3339Nano))}
	}

	_, err := t.dynamoClient.UpdateItem(params)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				// Failed condition check - not an error per say...
				return false, nil
			}
		}

		return false, err
	}

	// Item was successfully updated
	return true, nil
}

/*
MarkFileAsProcessing will create the tracking record (if needed), and mark it as processing.
Returns an indicator of the success or failure of doing this.

Return:
	-1 => Failure creating/updating the tracking record, this file should be attempted again later
	0 => File already processed, this file should be ignored
	1 => Record created/updated successfully, it is safe to process this file
*/
func (t *RecordTracker) MarkFileAsProcessing(file *data.FileToProcessInfo) int {
	// Grab Tracking Record
	record, err := t.GetRecord(file)
	if err != nil {
		log.Printf("Failed to query the tracking database with error: %s, will attempt again at a later date", err.Error())
		return -1
	}

	createdRecord := false
	if len(record.ID) == 0 { // Record doesn't exist - this is good, we can go ahead and create it!
		createdRecord, err = t.createRecord(file)
		if err != nil {
			log.Printf("Failed to create the tracking record in the tracking database with error: %s, will attempt again at a later date", err.Error())
			return -1
		}
	}

	if !createdRecord {
		// We need to check the state of the record to determine if we should re-attempt to process this file or not
		record, err := t.GetRecord(file)
		if err != nil {
			log.Printf("Failed to query the tracking database for the newly created record with error: %s, will attempt again at a later date", err.Error())
			return -1
		}

		if record.State == 1 {
			// This file is done, so we can simply ignore it!
			return 0
		} else if record.State == 0 {
			// Check the date of when it was started, if it was created more than 15 mins ago,
			//	we can assume previous attempt failed and try again
			// Otherwise, let's belay this attempt until later (in case we're just a little early checking)
			if record.StartTime.After(time.Now().Add(t.sixteenMinutes)) {
				return -1 // Try again later...
			}
		}

		// If we got here, then we're safe to proceed, so attempt to mark the record as started processing
		record.State = 0
		record.StartTime = time.Now()
		record.Expiry = time.Now().Add(t.expiryInterval)
		ok, err := t.setState(0, record.State, file, true)
		if err != nil {
			log.Printf("Failed to update the tracking record to retry processing again with error: %s, will try again later", err.Error())
			return -1
		} else if !ok {
			log.Printf("Tracking record has been updated outside this worker, will check again later if we need to re-process this file")
			return -1
		}
	}

	return 1
}

func (t *RecordTracker) MarkFileAsDone(file *data.FileToProcessInfo) (bool, error) {
	// Mark the record as done, if and only if the record is in the processing state (0)
	ok, err := t.setState(1, 0, file, false)
	if err != nil {
		log.Printf("Failed to update the tracking record to mark processing as done with error: %s", err.Error())
		return false, err
	}

	return ok, nil
}

func (t *RecordTracker) MarkFileAsFailed(file *data.FileToProcessInfo) (bool, error) {
	ok, err := t.setState(-1, 0, file, false)
	if err != nil {
		log.Printf("Failed to update the tracking record to mark processing as failed with error: %s", err.Error())
		return false, err
	}

	return ok, nil
}
