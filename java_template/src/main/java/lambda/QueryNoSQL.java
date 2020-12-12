package lambda;

import com.amazonaws.regions.Regions;
import com.amazonaws.regions.Region;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.*;
import com.amazonaws.services.*;
import com.amazonaws.services.lambda.runtime.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import saaf.Inspector;
import saaf.Response;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.*;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class QueryNoSQL implements RequestHandler<Request, HashMap<String, Object>> {

    private DynamoDB dynamoDb;

    // TODO: Fill this in
    private String DYNAMODB_TABLE_NAME = "Team9NoSQL";

    private Regions REGION = Regions.US_EAST_2;

    /**
     * Lambda Function Handler
     *
     * @param request Request POJO with defined variables from Request.java
     * @param context
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {

        // Create logger
        LambdaLogger logger = context.getLogger();

        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        AmazonDynamoDBClient client = this.initiDynamoDbClient();
        Table table = dynamoDb.getTable(DYNAMODB_TABLE_NAME);

        Map<String, AttributeValue> lastKeyEvaluated = null;
        do {
            ScanRequest scanRequest = new ScanRequest()
                    .withTableName("Team9NoSQL")
                    .withLimit(10)
                    .withExclusiveStartKey(lastKeyEvaluated);

            ScanResult result = client.scan(scanRequest);
            for (Map<String, AttributeValue> item : result.getItems()){
                System.out.println(item);
//                logger.log(item.keySet());
            }
            lastKeyEvaluated = result.getLastEvaluatedKey();
        } while (lastKeyEvaluated != null);


        //****************START FUNCTION IMPLEMENTATION*************************
        //Add custom key/value attribute to SAAF's output. (OPTIONAL)

        //Create and populate a separate response object for function output. (OPTIONAL)
        Response r = new Response();
        r.setValue("Test, worked!");

        inspector.consumeResponse(r);

        //****************END FUNCTION IMPLEMENTATION***************************

        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();

    }

    // int main enables testing function from cmd line
    public static void main(String[] args) {
        Context c = new Context() {
            @Override
            public String getAwsRequestId() {
                return "";
            }

            @Override
            public String getLogGroupName() {
                return "";
            }

            @Override
            public String getLogStreamName() {
                return "";
            }

            @Override
            public String getFunctionName() {
                return "";
            }

            @Override
            public String getFunctionVersion() {
                return "";
            }

            @Override
            public String getInvokedFunctionArn() {
                return "";
            }

            @Override
            public CognitoIdentity getIdentity() {
                return null;
            }

            @Override
            public ClientContext getClientContext() {
                return null;
            }

            @Override
            public int getRemainingTimeInMillis() {
                return 0;
            }

            @Override
            public int getMemoryLimitInMB() {
                return 0;
            }

            @Override
            public LambdaLogger getLogger() {
                return new LambdaLogger() {
                    @Override
                    public void log(String string) {
                        System.out.println("LOG:" + string);
                    }
                };
            }
        };

//        // Create an instance of the class
//        QueryNoSQL test = new QueryNoSQL();
//
//        // Create a request object
//        Request req = new Request();
//
//        // Grab the name from the cmdline from arg 0
//        String name = (args.length > 0 ? args[0] : "");
//
//        // Load the name into the request object
//        req.setName(name);
//
//        // Report name to stdout
//        System.out.println("cmd-line param name=" + req.getName());
//
//        // Run the function
//        //Response resp = lt.handleRequest(req, c);
//        Response resp = new Response();
//
//        // Print out function result
//        System.out.println("function result:" + resp.toString());
    }

    private AmazonDynamoDBClient initiDynamoDbClient() {
        AmazonDynamoDBClient client = new AmazonDynamoDBClient();
        client.setRegion(Region.getRegion(REGION));
        this.dynamoDb = new DynamoDB(client);
        return client;
    }

}