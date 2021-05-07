package main

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

func main() {

	fmt.Println(GetAllUsers(""))
}

//Table Structure
type UserInfo struct {

	// User Id  user
	UserId string `json:"userId,omitempty"`

	// First name of the logged user
	FirstName string `json:"firstName,omitempty"`

	// Last name of the logged user
	LastName string `json:"lastName,omitempty"`
}

//GetStoreTemplate details
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

//Advanced  Queries:
//New Table Structure
type UserInfoAdvanced struct {

	// User Id  user primary key
	UserId string `json:"userId,omitempty"`

	// First name of the logged user
	FirstName string `json:"firstName,omitempty"`

	// Last name of the logged user
	LastName string `json:"lastName,omitempty"`

	// BatchID
	BatchID string `json:"batchId,omitempty"`

	//Group Index created for this "groupIndex"
	Group string `json:"group,omitempty"`

	//Active sort key
	Active string `json:"active,omitempty"`
}

//GetStores Details based on filter, index and sort key
func GetAdvancedUsers(group, batch string) ([]UserInfoAdvanced, error) {
	users := []UserInfoAdvanced{}
	tableName := ""

	region := "us-east-2"
	awsSession, _ := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)

	dynaClient := dynamodb.New(awsSession)

	//To filter based on the batchID , filters can be any field other than primary key, sort key and index
	filterBatch := expression.Name("batchId").Equal(expression.Value(batch))

	//Required field to be avilable in the results

	proj := expression.NamesList(expression.Name("userId"), expression.Name("firstName"), expression.Name("lastName"),
		expression.Name("batchId"), expression.Name("group"), expression.Name("active"))
	expr, errExpression := expression.NewBuilder().
		WithFilter(filterBatch).
		WithProjection(proj).
		Build()
	if errExpression != nil {
		errorString := "Query Expression Error" + errExpression.Error()
		fmt.Println(errorString)
		return users, errExpression
	}

	var queryInput = &dynamodb.QueryInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		TableName:                 aws.String(tableName),
		IndexName:                 aws.String("groupIndex"),
		KeyConditions: map[string]*dynamodb.Condition{ //Only can add sort key, primary key or created index (GSI)
			"group": {
				ComparisonOperator: aws.String("EQ"),
				AttributeValueList: []*dynamodb.AttributeValue{
					{
						S: aws.String(group),
					},
				},
			},
			"active": {
				ComparisonOperator: aws.String("EQ"),
				AttributeValueList: []*dynamodb.AttributeValue{
					{
						S: aws.String("true"),
					},
				},
			},
		},
		FilterExpression:     expr.Filter(),
		ProjectionExpression: expr.Projection(),
	}

	var resp, errQueryDynamoDB = dynaClient.Query(queryInput)
	if errQueryDynamoDB != nil {
		errorString := "FailedTableLookupError" + errQueryDynamoDB.Error()
		fmt.Println(errorString)
		return users, errQueryDynamoDB
	}

	if len(resp.Items) > 0 {
		errUnMarshal := dynamodbattribute.UnmarshalListOfMaps(resp.Items, &users)
		if errUnMarshal != nil {
			errorString := "UnMarshal users Error" + errUnMarshal.Error()
			fmt.Println(errorString)
			return users, errUnMarshal
		}
	} else {
		errorString := "Stores Not found for Group: " + group + "BatchID : " + batch
		fmt.Println(errorString)
		return users, errors.New(errorString)
	}

	fmt.Println("Successfully Fetched " + strconv.FormatInt(*resp.Count, 10) + " results from DynamoDB for group: " + group)

	return users, nil
}

//GetListedUserss results of given primary key list (Max 100)
func GetListedUserss(userIDs []string) ([]UserInfoAdvanced, error) {

	users := []UserInfoAdvanced{}
	tableName := ""

	region := "us-east-2"
	awsSession, _ := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)

	dynaClient := dynamodb.New(awsSession)

	keys := make([]map[string]*dynamodb.AttributeValue, len(userIDs))
	itemKeyValue := make([]*dynamodb.AttributeValue, len(userIDs))

	for i := range userIDs {
		itemKeyValue[i] = &dynamodb.AttributeValue{
			S: aws.String(userIDs[i]),
		}
		itemMap := make(map[string]*dynamodb.AttributeValue, 1)
		itemMap["userId"] = itemKeyValue[i]
		keys[i] = itemMap

	}

	var queryInput = &dynamodb.BatchGetItemInput{
		RequestItems: map[string]*dynamodb.KeysAndAttributes{
			tableName: {
				Keys: keys,
			},
		},
	}

	var resp, errQueryDynamoDB = dynaClient.BatchGetItem(queryInput)
	if errQueryDynamoDB != nil {
		errorString := "FailedTableLookupError" + errQueryDynamoDB.Error()
		fmt.Println(errorString)
		return users, errQueryDynamoDB
	}

	if len(resp.Responses[tableName]) > 0 {
		errUnMarshal := dynamodbattribute.UnmarshalListOfMaps(resp.Responses[tableName], &users)
		if errUnMarshal != nil {
			errorString := "UnMarshal Error" + errUnMarshal.Error()
			fmt.Println(errorString)
			return users, errUnMarshal
		}
	} else {
		errorString := "Users Not found for the input list of ID's"
		fmt.Println(errorString)
		return users, errors.New(errorString)
	}

	fmt.Println("Successfully Fetched " + strconv.Itoa(len(users)) + " results from DynamoDB")
	return users, nil
}

//GetListed non primary key
func GetListedUberStores(storeIDs []string, group string) ([]UserInfoAdvanced, error) {
	users := []UserInfoAdvanced{}
	tableName := ""

	region := "us-east-2"
	awsSession, _ := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)

	dynaClient := dynamodb.New(awsSession)

	filterBatch := expression.Name("firstName").Equal(expression.Value(storeIDs[0]))

	for i := 1; i < len(storeIDs); i++ {
		filterBatch = filterBatch.Or(expression.Name("firstName").Equal(expression.Value(storeIDs[i])))
	}

	proj := expression.NamesList(expression.Name("userId"), expression.Name("firstName"), expression.Name("lastName"),
		expression.Name("batchId"), expression.Name("group"), expression.Name("active"))
	expr, errExpression := expression.NewBuilder().
		WithFilter(filterBatch).
		WithProjection(proj).
		Build()
	if errExpression != nil {
		errorString := "Query Expression Error" + errExpression.Error()
		fmt.Println(errorString)
		return users, errExpression
	}

	var queryInput = &dynamodb.QueryInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		TableName:                 aws.String(tableName),
		IndexName:                 aws.String("groupIndex"),
		KeyConditions: map[string]*dynamodb.Condition{
			"group": {
				ComparisonOperator: aws.String("EQ"),
				AttributeValueList: []*dynamodb.AttributeValue{
					{
						S: aws.String(group),
					},
				},
			},
			"active": {
				ComparisonOperator: aws.String("EQ"),
				AttributeValueList: []*dynamodb.AttributeValue{
					{
						S: aws.String("true"),
					},
				},
			},
		},
		FilterExpression:     expr.Filter(),
		ProjectionExpression: expr.Projection(),
	}

	var resp, errQueryDynamoDB = dynaClient.Query(queryInput)
	if errQueryDynamoDB != nil {
		errorString := "FailedTableLookupError" + errQueryDynamoDB.Error()
		fmt.Println(errorString)
		return users, errQueryDynamoDB
	}

	if len(resp.Items) > 0 {
		errUnMarshal := dynamodbattribute.UnmarshalListOfMaps(resp.Items, &users)
		if errUnMarshal != nil {
			errorString := "UnMarshal  Error" + errUnMarshal.Error()
			fmt.Println(errorString)
			return users, errUnMarshal
		}
	} else {
		errorString := "Stores Not found for group: " + group + " in the input list of ID's"
		fmt.Println(errorString)
		return users, errors.New(errorString)
	}

	fmt.Println("Successfully Fetched " + strconv.FormatInt(*resp.Count, 10) + " results from DynamoDB for Pos Org ID: " + group)

	return users, nil
}
