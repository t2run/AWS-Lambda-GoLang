
package main

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func main() {

	fmt.Println(GetAllUsers(""))
}

//Table Structure
type UserInfo struct {

	// UserId  
	UserId string `json:"userId,omitempty"`

	// First name 
	FirstName string `json:"firstName,omitempty"`

	// Last name 
	LastName string `json:"lastName,omitempty"`
}

//GetUser details
func GetUser(tableName, userID string) (UserInfo, error) {

	var userInfo UserInfo
	region := "us-east-2"
	awsSession, _ := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)

	dynaClient := dynamodb.New(awsSession)

	keys := make(map[string]*dynamodb.AttributeValue)
	itemKeyValue := dynamodb.AttributeValue{S: aws.String(userID)}
	//Primary key
	keys["userId"] = &itemKeyValue

	getItemInput := dynamodb.GetItemInput{TableName: aws.String(tableName), Key: keys}
	response, errFromLookup := dynaClient.GetItem(&getItemInput)
	if errFromLookup != nil {
		errorString := "FailedTableLookupError" + "[" + errFromLookup.Error() + "]"
		fmt.Println(errorString)
		return userInfo, errors.New(errorString)
	}
	if response.Item == nil {
		errorString := "UserNotFound" + ": " + userID
		fmt.Println(errorString)
		return userInfo, errors.New(errorString)
	}

	errFromItemUnmarshal := dynamodbattribute.UnmarshalMap(response.Item, &userInfo)
	if errFromItemUnmarshal != nil {
		errorString := "ItemUnMarshalError" + ": " + userID
		fmt.Println(errorString)
		return userInfo, errors.New(errorString)
	}

	fmt.Println(" User details of userID : " + userID + " Fetched Successfully")

	return userInfo, nil
}

//GetAllUsers Details
func GetAllUsers(tableName string) ([]UserInfo, error) {

	users := []UserInfo{}
	region := "us-east-2"
	awsSession, _ := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)

	dynaClient := dynamodb.New(awsSession)

	var queryInput = &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	}

	var resp, errQueryDynamoDB = dynaClient.Scan(queryInput)
	if errQueryDynamoDB != nil {
		errorString := "Failed to Lookup table" + errQueryDynamoDB.Error()
		fmt.Println(errorString)
		return users, errQueryDynamoDB
	}

	if len(resp.Items) > 0 {
		errUnMarshal := dynamodbattribute.UnmarshalListOfMaps(resp.Items, &users)
		if errUnMarshal != nil {
			errorString := "UnMarshal Stores Error" + errUnMarshal.Error()
			fmt.Println(errorString)
			return users, errUnMarshal
		}
	} else {
		errorString := "Users Not found"
		fmt.Println(errorString)
		return users, errors.New(errorString)
	}

	fmt.Println("Successfully Fetched " + strconv.FormatInt(*resp.Count, 10))

	return users, nil
}

//CreateNewUser
func CreateNewUser(userTableName string, userInfo UserInfo) (UserInfo, error) {
	var user UserInfo

	region := "us-east-2"
	awsSession, _ := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)

	dynaClient := dynamodb.New(awsSession)

	// Save Store
	inputItemValue, errMarshalMap := dynamodbattribute.MarshalMap(userInfo)
	if errMarshalMap != nil {
		errorString := "Marshal Map Error" + "[" + errMarshalMap.Error() + "]"
		fmt.Println(errorString)
		return user, errors.New(errorString)
	}

	input := &dynamodb.PutItemInput{
		Item:      inputItemValue,
		TableName: aws.String(userTableName),
	}

	_, errPutItem := dynaClient.PutItem(input)
	if errPutItem != nil {
		errorString := "Put Item Error" + "[" + errPutItem.Error() + "]"
		fmt.Println(errorString)
		return user, errors.New(errorString)
	}
	fmt.Println("User : " + user.UserId + " Created Successfully")
	return user, nil
}

//UpdateUserInfo in DynamoDB Store Details
func UpdateUserInfo(userTableName string, userInfo UserInfo) (UserInfo, error) {

	region := "us-east-2"
	awsSession, _ := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)

	dynaClient := dynamodb.New(awsSession)

	//UserInfoUpdate model
	type UserInfoUpdate struct {
		FirstName string `json:":firstName,omitempty"`
		LastName  string `json:":lastName,omitempty"`
	}

	//RoleInfoKey model
	type RoleInfoKey struct {
		RoleName string `json:"roleName"`
	}

	//UserInfoItemKey model
	type UserInfoKey struct {
		UserID string `json:"userId"`
	}

	updateUserInfo := UserInfoUpdate{
		FirstName: userInfo.FirstName,
		LastName:  userInfo.LastName,
	}

	av, KeyErr := dynamodbattribute.MarshalMap(UserInfoKey{UserID: userInfo.UserId})
	if KeyErr != nil {
		errorString := "FailedTableLookupError" + "[" + KeyErr.Error() + "]"
		fmt.Println(errorString)
		return userInfo, errors.New(errorString)
	}

	updateDetails, errUpdateDetails := dynamodbattribute.MarshalMap(updateUserInfo)
	if errUpdateDetails != nil {
		errorString := "FailedToCreateUpdateDetails" + "[" + errUpdateDetails.Error() + "]"
		fmt.Println(errorString)
		return userInfo, errors.New(errorString)
	}

	input := &dynamodb.UpdateItemInput{
		Key:       av,
		TableName: aws.String(userTableName),
		// ExpressionAttributeNames:  map[string]*string{"#role": aws.String("role")},
		UpdateExpression:          aws.String("set firstName = :firstName, lastName = :lastName"),
		ExpressionAttributeValues: updateDetails,
	}

	_, errUpdateItem := dynaClient.UpdateItem(input)
	if errUpdateItem != nil {
		errorString := "UpdateItemError" + "[" + errUpdateItem.Error() + "]"
		fmt.Println(errorString)
		return userInfo, errors.New(errorString)
	}

	fmt.Println("User : " + userInfo.UserId + " Details Updated Successfully")
	return userInfo, nil
}

//DeleteUser from the table
func DeleteUser(userID, userTableName string) error {
	region := "us-east-2"
	awsSession, _ := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)

	dynaClient := dynamodb.New(awsSession)

	keys := make(map[string]*dynamodb.AttributeValue)
	itemKeyValue := dynamodb.AttributeValue{S: aws.String(userID)}
	keys["userId"] = &itemKeyValue

	deleteItemInput := dynamodb.DeleteItemInput{TableName: aws.String(userTableName), Key: keys}
	_, errFromDelete := dynaClient.DeleteItem(&deleteItemInput)
	if errFromDelete != nil {
		errorString := "Failed to Delete" + "[" + errFromDelete.Error() + "]"
		fmt.Println(errorString)
		return errors.New(errorString)
	}
	fmt.Println("User : " + userID + " Deleted Successfully")
	return nil
}
