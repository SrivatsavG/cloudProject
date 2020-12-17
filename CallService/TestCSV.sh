#!/bin/bash
# JSON object to pass to Lambda Function


bucketname="team9-s3"
filenameSrc="test.csv"
filenameDest="testTransform.csv"
totalRecords="10000"

json='{"bucketname":"'"$bucketname"'","filenameSrc":"'"$filenameSrc"'","filenameDest":"'"$filenameDest"'","totalRecords":"'"$totalRecords"'"}'


echo "Invoking Transform Lambda function using API Gateway"

time output=`curl -s -H "Content-Type: application/json" -X POST -d $json https://rouc2pmqod.execute-api.us-east-2.amazonaws.com/dev`
echo ""

echo ""
echo "JSON RESULT:"
echo $output | jq 
echo ""


