#!/bin/bash
# JSON object to pass to Lambda Function


bucketname="team9-s3"
filename="transformedData.csv"

json='{"bucketname":"'"$bucketname"'","filename":"'"$filename"'"}'

echo "Invoking LoadToSQL Lambda function using API Gateway"

time output=`curl -s -H "Content-Type: application/json" -X POST -d $json https://0os20s0079.execute-api.us-east-2.amazonaws.com/dev`
echo ""

echo ""
echo "JSON RESULT:"
echo $output | jq 
echo ""


