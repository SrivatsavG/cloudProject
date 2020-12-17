#!/bin/bash
# JSON object to pass to Lambda Function


bucketname="team9-s3"
filename="transformedData.csv"
dynamoDBTableName="Team9NoSQL"
#REGION IS HARD CODED IN THE JAVA FILE TO US-EAST-2

json='{"bucketname":"'"$bucketname"'","filename":"'"$filename"'","dynamoDBTableName":"'"$dynamoDBTableName"'"}'

#echo "Invoking LoadToSQL Lambda function using API Gateway"

#time output=`curl -s -H "Content-Type: application/json" -X POST -d $json https://0os20s0079.execute-api.us-east-2.amazonaws.com/dev`
echo ""

echo "Invoking Lambda function using AWS CLI"

time output=`aws lambda invoke --invocation-type RequestResponse --function-name LoadNoSQL --region us-east-2 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`




echo ""
echo "JSON RESULT:"
echo $output | jq 
echo ""


