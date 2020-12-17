package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import saaf.Inspector;
import saaf.Response;
import java.util.HashMap;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.*;
import java.nio.*;
import java.nio.charset.*;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.Collections;
import java.util.Map;
import java.util.EventObject;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.List;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.util.StringUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.*;
import com.amazonaws.services.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
//import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 * @author Siddharth Sheth
 * @author Srivastav Gopalakrishnan
 * @author Patrick Moy
 */
public class TransformCSV implements RequestHandler<Request, HashMap<String, Object>> {

    /**
     * Lambda Function Handler takes .csv file from S3 and transforms, creating
     * a new .csv to store in S3
     *
     * @param request Request POJO with defined variables from Request.java
     * @param context
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {

        LambdaLogger logger = context.getLogger();

        AmazonS3 s3ClientRead = AmazonS3ClientBuilder.standard().build();
        AmazonS3 s3ClientWrite = AmazonS3ClientBuilder.standard().build();
        String bucketname = request.getBucketname();
        String filenameSrc = request.getFilenameSrc();
        String filenameDest = request.getFilenameDest();
        int totalRecords = Integer.parseInt(request.getTotalRecords());

        //get object file using source bucket and srcKey name
        S3Object s3Object = s3ClientRead.getObject(new GetObjectRequest(bucketname, filenameSrc));
        //get content of the file

        InputStream objectData = s3Object.getObjectContent();
        //scanning data line by line

        // Processing .csv
        StringWriter sw;
        try ( Scanner scanner = new Scanner(objectData)) {
            sw = new StringWriter();
            logger.log("Start While Loop");
            int recordCount = 0;
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                // RegEx handles splitting when some values contain commas (using lookahead)
                String[] arrOfStr = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                for (int i = 0; i < arrOfStr.length; i++) {
                     String str = arrOfStr[i];
                    if (i == 4) {
                        // Transforms listed gender to single-char representation
                        if (str.equals("Male")) {
                            str = "M";
                        } else if (str.equals("Female")) {
                            str = "F";
                        } else if (str.equals("Other") || str.equals("Missing") || str.equals("Unknown")) {
                            str = "U";
                        }
                    } else if (i == 5) {
                        if (str.equals("0 - 9 Years") || str.equals("10-19")) {
                            str = "Young";
                        } else if (str.equals("10 - 19 Years") || str.equals("20 - 29 Years") || str.equals("30 - 39 Years") || str.equals("40 - 49 Years") || str.equals("50 - 59 Years")) {
                            str = "Adult";
                        } else {
                            str = "Elderly";
                        }
                    }

                    sw.append(str);
                    if (i != arrOfStr.length - 1) {
                        sw.append(",");
                    }
                }
                sw.append("\n");
                recordCount++;
                if (recordCount == totalRecords) {
                    break;
                }
            }
        }

        logger.log("Transformed CSV");

//------------------------------------------------------------
        //CREATE NEW CSV
        logger.log("Start Write CSV");

        byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
        InputStream is = new ByteArrayInputStream(bytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        meta.setContentType("text/plain");

        s3ClientWrite.putObject(bucketname, filenameDest, is, meta);

        logger.log("Write CSV completed");

//--------------------------------------------------
        Response response = new Response();
        response.setValue("Bucket:" + bucketname + "processed");
        Inspector inspector = new Inspector();
        inspector.consumeResponse(response);
        logger.log("Response created");
        return inspector.finish();
    }
}
