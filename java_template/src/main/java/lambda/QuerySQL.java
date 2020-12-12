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
import saaf.Inspector;
import java.util.HashMap;
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

import java.nio.charset.Charset;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class QuerySQL implements RequestHandler<Request, HashMap<String, Object>> {

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
        int j = 1;
        //****************START FUNCTION IMPLEMENTATION*************************
        LambdaLogger logger = context.getLogger();
        Response response = new Response();

        try {

            String url = "jdbc:mysql://team9-rds.cluster-c1egvakjnwad.us-east-2.rds.amazonaws.com:3306/TEST";
            String username = "team9";
            String password = "team9Password";
            String driver = "com.mysql.cj.jdbc.Driver";

            logger.log("Username: " + username);
            logger.log("url: " + url);
            logger.log("password: " + password);
            logger.log("driver: " + driver);

            logger.log("Connecting to DB");

            try ( Connection con = DriverManager.getConnection(url, username, password)) {
                logger.log("Connection successful");

                //response.setValue("Hello, THIS WAS SUCCESSFUL!!");
                // Query mytable to obtain full resultset
                PreparedStatement ps = con.prepareStatement("select * from mytable;");
                ResultSet rs = ps.executeQuery();

                // Load query results for [name] column into a Java Linked List
                // ignore [col2] and [col3] 
                LinkedList<String> list = new LinkedList<String>();

                while (rs.next()) {
                    //logger.log("col1=" + rs.getString("cdc_report_dt"));
                    list.add(rs.getString("cdc_report"));
                    list.add(rs.getString("pos_spec_dt"));
                    list.add(rs.getString("onset_dt"));
                    list.add(rs.getString("current_status"));
                    list.add(rs.getString("sex"));
                    list.add(rs.getString("age_group"));
                    list.add(rs.getString("race"));
                    list.add(rs.getString("hosp_yn"));
                    list.add(rs.getString("icu_yn"));
                    list.add(rs.getString("death_yn"));
                    list.add(rs.getString("medcond_yn"));

                    logger.log("Record Start: " + j);
                    logger.log("col1=" + rs.getString("cdc_report"));
                    logger.log("col2=" + rs.getString("pos_spec_dt"));
                    logger.log("col3=" + rs.getString("onset_dt"));
                    logger.log("col4=" + rs.getString("current_status"));
                    logger.log("col5=" + rs.getString("sex"));
                    logger.log("col6=" + rs.getString("age_group"));
                    logger.log("col7=" + rs.getString("race"));
                    logger.log("col8=" + rs.getString("hosp_yn"));
                    logger.log("col9=" + rs.getString("icu_yn"));
                    logger.log("col10=" + rs.getString("death_yn"));
                    logger.log("col11=" + rs.getString("medcond_yn"));

                    logger.log("Record End: " + j);
                    j++;
                }

                //j++;
                rs.close();
                con.close();

                /* for (String data:list){
                                logger.log(data);
                           }*/
                response.setValue("QUERYSQL Successful");
                response.setValue("list");

            }

        } catch (Exception e) {
            logger.log("Got an exception working with MySQL! ");
            logger.log(e.getMessage());
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            logger.log(sw.toString());
            response.setValue("QUERYSQL FAILED!!");
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
