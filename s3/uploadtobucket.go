package main

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {
	UploadtoS3()
}

//UploadtoS3 bucket
func UploadtoS3() {

	bucketName := ""
	objectKey := ""

	payload := ""

	region := "us-east-2"
	awsSession, _ := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)

	svc := s3.New(awsSession)

	s3BucketInput := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(strings.NewReader(string(payload))),
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	}

	_, err := svc.PutObject(s3BucketInput)
	if err != nil {
		errorString := "Error while Uploading to S3 Bucket" + "[" + err.Error() + "]"
		fmt.Println(errorString)
	}
}
