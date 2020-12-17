#!/bin/bash
# JSON object to pass to Lambda Function


bucketname="team9-s3"
filename="transformedData.csv"

json='{"bucketname":"'"$bucketname"'","filename":"'"$filename"'"}'

echo "Invoking LoadToSQL Lambda function using API Gateway"
time output=`curl -s -H "Content-Type: application/json" -X POST -d $json https://a9dnv1fprj.execute-api.us-east-2.amazonaws.com/dev`


#echo "Invoking Lambda function using AWS CLI"

#time output=`aws lambda invoke --invocation-type RequestResponse --function-name team9-testLambda --region us-east-2 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`

echo ""

echo ""
echo "JSON RESULT:"
echo $output | jq 
echo ""


