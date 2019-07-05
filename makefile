SHELL := /bin/bash


S3_BUCKET := $(shell echo $$ARTIFACT_BUCKET)
STACK_PARAMS := $(shell echo $$PARAMS)
STACK_NAME := flowlogs-merger
REGION := us-east-1

PROJECTNAME := $(shell basename "$(PWD)")

GOBASE := $(shell pwd)
GOPATH := $(GOBASE)/vendor:$(GOBASE)
GOBIN := $(GOBASE)/bin

GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOVET=$(GOCMD) vet
GOGET=$(GOCMD) get
GOLINT=golint -set_exit_status

BINARY_NAME=bin/flowlogs-merger
LAMBDA_BINARY=bin/lambda-flowlogs-merger

all: lint vet test build build-lambda deploy

foo: 
	@echo DUDE: $(STACK_PARAMS)
build: 
		$(GOBUILD) -o $(BINARY_NAME)

build-lambda:
		GOOS=linux GOARCH=amd64 $(GOBUILD) -o $(LAMBDA_BINARY)

test: 
		$(GOTEST) -bench -v ./...
lint: 
		$(GOLINT)
vet: 
		$(GOVET) .

clean: 
		$(GOCLEAN)
		rm -f $(BINARY_NAME)
		rm -f $(LAMBDA_BINARY)

deploy: 
		sam package --template-file template.yaml --s3-bucket $(S3_BUCKET) --output-template-file packaged.yaml --region=$(REGION)
		sam deploy --template-file ./packaged.yaml --stack-name $(STACK_NAME) --capabilities CAPABILITY_IAM --region=$(REGION) --parameter-overrides $(STACK_PARAMS)
		rm -f packaged.yaml

		
run:
		$(GOBUILD) -o $(BINARY_NAME) -v
		USE_REGION=$(REGION) ./$(BINARY_NAME)

deps:
		$(GOGET) github.com/aws/aws-sdk-go
		$(GOGET) github.com/aws/aws-lambda-go/lambda
		$(GOGET) github.com/satori/go.uuid
		$(GOGET) golang.org/x/text/message
		$(GOGET) github.com/xitongsys/parquet-go/...
