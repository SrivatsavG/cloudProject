package lambda;

import saaf.Inspector;
import saaf.Response;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import java.util.Scanner;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;

import java.nio.charset.Charset;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class ProcessCSVForNoSQL implements RequestHandler<Request, HashMap<String, Object>> {

    /**
     * Lambda Function Handler
     *
     * @param request Request POJO with defined variables from Request.java
     * @param context
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    
    
    public HashMap<String, Object> handleRequest(Request request, Context context) {

        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        //****************START FUNCTION IMPLEMENTATION*************************
        LambdaLogger logger = context.getLogger();
        Response response = new Response();

        //S3 SETUP
        String bucketname = request.getBucketname();
        String filename = request.getFilename();
        logger.log("Bucketname is " + bucketname);
        logger.log("Filename is " + filename);
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        //get object file using source bucket and srcKey name
        logger.log("AmazonS3 s3Client line works");
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketname, filename));

        logger.log("S3Object s3Object line works");
        //get content of the file
        InputStream objectData = s3Object.getObjectContent();

        logger.log("InputStream objectData line works");

        logger.log("S3 access is successful");

        //----------------------NO SQL SETUP----------------------------
       
        String region = "US_EAST_2";
        String tableName = "test-nosql";
        
        try {
            
            
            final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();
                
            
            AmazonDynamoDBClient client = new AmazonDynamoDBClient();
            client.setRegion(Region.getRegion(REGION));
            this.dynamoDb = new DynamoDB(client);
            logger.log("Connecting to NoSQL DB");

            try ( Connection con = DriverManager.getConnection(url, username, password)) {
                logger.log("Connection successful");
                Scanner scanner = new Scanner(objectData);
                int i = 0;
                while (scanner.hasNext()) {

                    String line = scanner.nextLine();
                    if (i != 0) {
                        String[] arrOfStr = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                        //("insert into mytable values('" + request.getName() + "','b','c');");-- Tutorial 6 reference
                        i++;
                        //PreparedStatement ps = con.prepareStatement("insert into mytable values('" + request.getName() + "','b','c');");
                        PreparedStatement ps = con.prepareStatement("insert into mytable values('" + arrOfStr[0] + "','" + arrOfStr[1] + "','" + arrOfStr[2] + "','" + arrOfStr[3] + "','" + arrOfStr[4] + "','" + arrOfStr[5] + "','" + arrOfStr[6] + "','" + arrOfStr[7] + "','" + arrOfStr[8] + "','" + arrOfStr[9] + "','" + arrOfStr[10] + "');");
                        ps.execute();
                    }
                    i++;

                }
                scanner.close();
                logger.log("Writing to SQL successful");
                response.setValue("Hello, THIS WAS SUCCESSFUL!!");
            }

        } catch (Exception e) {
            logger.log("Got an exception working with MySQL! ");
            logger.log(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.log(sw.toString());
            response.setValue("THIS FAILED!!");
        }

        //scanning data line by line
        //Create and populate a separate response object for function output. (OPTIONAL)
        inspector.consumeResponse(response);

        //****************END FUNCTION IMPLEMENTATION***************************
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
}
