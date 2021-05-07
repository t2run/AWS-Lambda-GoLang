package main

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/secretsmanager"
)

func main() {
	fmt.Println(GetSecrets("secretkey"))
}

//GetSecrets for the secret key
func GetSecrets(secretKey string) (map[string]string, error) {

	result := make(map[string]string)

	region := "us-east-2"
	awsSession, _ := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)

	svc := secretsmanager.New(awsSession)

	input := secretsmanager.GetSecretValueInput{SecretId: &secretKey}
	output, errFromSvc := svc.GetSecretValue(&input)

	if errFromSvc != nil {
		fmt.Println(errFromSvc.Error())
		return nil, errFromSvc
	}
	if output.SecretString == nil {
		errorString := "Unable to find the secrets from the secret manager for the key: " + secretKey
		fmt.Println(errorString)
		return nil, errors.New(errorString)
	}

	errFromUnmarshal := json.Unmarshal([]byte(*output.SecretString), &result)

	if errFromUnmarshal != nil {
		fmt.Println(errFromUnmarshal.Error())
		return nil, errFromUnmarshal
	}

	return result, nil
}
