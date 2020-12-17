package lambda;

import java.util.HashMap;
import java.util.Iterator;

import java.io.*;
import java.util.*;

import com.amazonaws.regions.Regions;
import com.amazonaws.regions.Region;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

import com.amazonaws.*;
import com.amazonaws.services.*;
import com.amazonaws.services.lambda.runtime.*;
import saaf.Inspector;
import saaf.Response;

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
        logger.log("Connection successful");

        //SELECT * EQUIVALENT
        long startTime = System.nanoTime();
        ScanSpec scanSpec = new ScanSpec().withProjectionExpression("cdc_report, pos_spec_dt, onset_dt,current_status,sex,age_group,race,hosp_yn,icu_yn,death_yn,medcond_yn");
        long stopTime = System.nanoTime();
        long elapsedTime1 = stopTime - startTime;
        

        //SELECT * FROM WHERE EQUIVALENT
        HashMap<String, Object> valueMap = new HashMap<String, Object>();
        valueMap.put(":M", "M");
        QuerySpec querySpec = new QuerySpec()
                .withKeyConditionExpression("sex = :M")
                .withValueMap(valueMap);
        ItemCollection<QueryOutcome> items = null;
        long elapsedTime2 = 0; 
        try {
            startTime = System.nanoTime();
            items = table.query(querySpec);
            stopTime = System.nanoTime();
            elapsedTime2 = stopTime - startTime;            
        } catch (Exception e) {
            System.err.println("Unable to query movies from 1985");
            System.err.println(e.getMessage());
        }
        
        logger.log("Query Select ALL run-time " + elapsedTime1);
        logger.log("Query Select ALL sex = M " + elapsedTime2);

        //****************START FUNCTION IMPLEMENTATION*************************
        //Add custom key/value attribute to SAAF's output. (OPTIONAL)
        //Create and populate a separate response object for function output. (OPTIONAL)
        Response r = new Response();

        r.setValue("Test, worked!");

        inspector.consumeResponse(r);

        //****************END FUNCTION IMPLEMENTATION***************************
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        logger.log("NoSQL query successful");
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
