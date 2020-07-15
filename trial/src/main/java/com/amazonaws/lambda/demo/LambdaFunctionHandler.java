package com.amazonaws.lambda.demo;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TimeZone;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.amazonaws.services.simpleemail.model.Body;
import com.amazonaws.services.simpleemail.model.Content;
import com.amazonaws.services.simpleemail.model.Destination;
import com.amazonaws.services.simpleemail.model.Message;
import com.amazonaws.services.simpleemail.model.SendEmailRequest;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/*
 
 1.Read the contents of S3 
 2.Parse it and Extract errorNumber, errorDescription, VersionNumber 
 3.Using errorNumber obtain the errorCategory from RDS Table 
 4.Check in RDS if the error is present
 5.If not present create error in RDS
 6.If present or not present create in DynamoDB
 7.Send Email 
 
 */
public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {
	
	/* Amazon Services Client */
    AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
          .build();
    private AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard().build();
    AmazonSimpleEmailService client = AmazonSimpleEmailServiceClientBuilder.standard()
          .build();
    
    /* DynamoDB Details*/
    //private String tableDynamoDBName="KonnectError";
    
    /* Email Details*/
    static final String FROM = "hemalathavhavale@gmail.com";  // Replace with your "From" address. This address must be verified.
    static final String TO = "hemalvj@gmail.com"; // Replace with a "To" address. 
    static String BODY = "Hello, Please find the error identified through customer call center audio  .";
    static final String SUBJECT = "Errors from SmartKonnectors";
    
    /*Details to be inserted into DB*/
    private String errorDescription="";
    private int errorNumber=0;
    private String versionNumber="1.0";
    private boolean errorPresent=false;
    private String errorCategory="NoCategory";
    private String productId="X000";
    
    /*RDS Details*/
    private String url = "jdbc:mysql://errordb.csij8k1jeoez.us-east-1.rds.amazonaws.com:3306/errordb";
    private String username = "admin";
    private String password = "rootroot";

    /*S3 job Details*/
    String transcript="";
    String jobName="";
    String accountId="";
    
    public LambdaFunctionHandler() {}
    
    
    @Override
    public String handleRequest(S3Event event, Context context) {
    	JSONParser jsonParser = new JSONParser();
    	
        /* Read from S3 OBject */
    	S3Object fullObject = null;
        context.getLogger().log("Received event: " + event);
        try {
	        String bucket = event.getRecords().get(0).getS3().getBucket().getName();
	        String key = event.getRecords().get(0).getS3().getObject().getKey();
	        fullObject = s3Client.getObject(new GetObjectRequest(bucket, key));
	        S3ObjectInputStream obj=fullObject.getObjectContent();
	        BufferedReader reader = new BufferedReader(new InputStreamReader(obj));
            JSONObject jsonObject = (JSONObject) jsonParser.parse(reader.readLine());
        	jobName  = (String) jsonObject.get("jobName");
        	accountId  = (String) jsonObject.get("accountId");
        	JSONObject results  = (JSONObject) jsonObject.get("results");
        	JSONArray transcripts = (JSONArray) results.get("transcripts");
        	context.getLogger().log(" JobName: " + jobName);
			
        	Iterator<JSONObject> itr = transcripts.iterator();
        	while (itr.hasNext()) {
        		Object jsonObject1 = itr.next();
				JSONObject jsonObject2 = (JSONObject) jsonObject1;
				transcript = (String) jsonObject2.get("transcript");
				context.getLogger().log(" Transcript: " + transcript);
		    }
        	
	        /* 1. Check for error_no, error code, then pickup next 20 words for error number and description */
	        String temp="";
	        String token="";
	        int errorIndex=transcript.indexOf("error");
	        if (errorIndex==-1)
	        	errorIndex=transcript.indexOf("issue");
	        if (errorIndex==-1)
	        	errorIndex=transcript.indexOf("problem");
	        if (errorIndex==-1)
	    	errorIndex=transcript.indexOf("facing");
	             	
		    if (errorIndex!=-1) {
		    	temp = transcript.substring(errorIndex);
		    	StringTokenizer st = new StringTokenizer(temp);
		    	int count=20;
		    	while(st.hasMoreTokens() & count>0)
				{
		    		   token = st.nextToken();
					   errorDescription  = errorDescription + token + " ";
					   count=count-1;
					  try {
						  errorNumber=Integer.parseInt(token);
					  }catch(Exception e) {
					  }
				}
		    	errorDescription=errorDescription.replace("\'", "\'\'");
		    	context.getLogger().log(" ErrorNumber & ErrorDescription: " + errorNumber + " & " + errorDescription);
		    }
        
        /* 2. Check if version number is present */
        int versionIndex=transcript.indexOf("version");
    	temp="";
    	token="";
        if (versionIndex!=-1) {
	    
	    	temp = transcript.substring(versionIndex);
	    	StringTokenizer st = new StringTokenizer(temp);
	    	int count=5;
	    	while(st.hasMoreTokens() & count>0)
			{
	    		   token = st.nextToken();
				   count=count-1;
				   try {
						  Float.parseFloat(token);
						  versionNumber=token;
					  }catch(Exception e) {
					  }
				 
			}
	    	context.getLogger().log(" Version Number: " + versionNumber);
	    }
        
        /* 3. Check the error category */
       
        errorCategory=readRdsCategory();
        context.getLogger().log(" errorCategory: " + errorCategory);
        
        
        /* 3. Check in RDS if the error is present */
         errorPresent=checkErrorinRDS();	
         context.getLogger().log(" errorPresent: " + errorPresent);
        
        /* 5. Create Error in RDS if the error is not present */
         Boolean error_flag=true;
         if (errorPresent==false) {
             if(createRDSError()==true) {
            	 context.getLogger().log(" createRDSError: " + "success");
            	 /* 3. Send Email */
            	 sendEmail();
            	 context.getLogger().log(" Email sent!");
            	
             }else {
            	 context.getLogger().log(" createRDSError: " + "failure");
            	 error_flag=false;
             }
         }
         	
        /* Create Error in DynamoDB only if RDS is success or RDS error is not created */
        if (error_flag==true)
        	createDynamoDBError();
        context.getLogger().log(" createDynamoDBError: " + "success");
        
     } catch (Exception e) {
    	 context.getLogger().log("Failure");
 		 context.getLogger().log(e.getMessage());
     }
        return "success";
    }

    public String readRdsCategory() {
   	 
   	 try {
   		  
   	      Connection conn = DriverManager.getConnection(url, username, password);
   	      Statement stmt = conn.createStatement();
   	      int temp = (errorNumber/100) * 100;
   	      ResultSet resultSet = stmt.executeQuery("SELECT error_category from error_category where error_num = " + temp);
   	      while (resultSet.next()) {
   	         errorCategory = resultSet.getObject("error_category").toString();
   	      }
   	      conn.close();
   	 } catch (Exception e) {
   	      e.printStackTrace();
   	 }
   	 return errorCategory;
   }
   
  
  public void createDynamoDBError() {
       String err=String.valueOf(errorNumber);
   	   String errPresent=String.valueOf(errorPresent);
       Map<String, AttributeValue> values = new HashMap<String, AttributeValue>();
       values.put("errorCode", new AttributeValue(err));
       values.put("errorDescription", new AttributeValue(errorDescription));
       values.put("errorCategory", new AttributeValue(errorCategory));
       values.put("versionNumber", new AttributeValue(versionNumber));
       values.put("errorPresent", new AttributeValue(errPresent));
       values.put("productId", new AttributeValue(productId));
       Date date = new Date();
       TimeZone tz = TimeZone.getTimeZone("UTC");
       DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
       df.setTimeZone(tz);
       String date_time = df.format(date);
       values.put("date_time", new AttributeValue(date_time));
       PutItemResult response = this.dynamoDB.putItem("KonnectErrorCode", values);
       PutItemResult response1 = this.dynamoDB.putItem("KonnectErrorCategory", values);
       PutItemResult response2 = this.dynamoDB.putItem("KonnectProductVersion", values);
       PutItemResult response3 = this.dynamoDB.putItem("KonnectDateTime", values);
       PutItemResult response4 = this.dynamoDB.putItem("KonnectRDSMainFlag", values);
       
  }
  
  
  public boolean createRDSError() {
	   boolean errorCreated = false;
		 
	  	 try {
	  		 
	  		 Connection conn = DriverManager.getConnection(url, username, password);
	  	     Statement stmt = conn.createStatement();
	  	     stmt = conn.createStatement();
	         String sql = "INSERT INTO konnect_error (error_number,error_code, error_source,error_description, error_category, source_version,product_id,created_date)" +
	                     "VALUES (0," +                                //errornumber					//errornumberautogenerated
	                     		  "'" + errorNumber +"'," +              //customererrornumber
	                     		  "'" + jobName +"'," +                  //jobName
	  	    		              "'" + errorDescription + "'," +     //errordescription
	  	    		              "'" + errorCategory + "'," +			//errorcategory
	  	    		              "'" + versionNumber + "'," +          //versionnumber
	  	    		              "'" + productId + "'," +              //productid
	  	    		              "curdate());" ;                      //createddate
	  	     		              					
	  	     stmt.executeUpdate(sql);
	  	     errorCreated=true;
	  	     conn.close();
	  	 } catch (Exception e) {
	  	      e.printStackTrace();
	  	 }
	  	 return errorCreated;
	   
  }
  public boolean checkErrorinRDS( ){
	 
	 boolean errorPresent = false;
	 String errorDes_db;
 	 try {
 		  
 	      Connection conn = DriverManager.getConnection(url, username, password);
 	      Statement stmt = conn.createStatement();
 	      ResultSet resultSet = stmt.executeQuery("SELECT error_description from konnect_error_final");
 	      while (resultSet.next()) {
 	         errorDes_db = resultSet.getObject("error_description").toString();
 	         errorPresent = errorDescription.contains(errorDes_db);
 	         if (errorPresent == true) {
 	        	return errorPresent;
 	         }
 	      }
 	      conn.close();
 	 } catch (Exception e) {
 	      e.printStackTrace();
 	 }
 	 return errorPresent;
  }

	public void sendEmail() {
		  BODY = BODY  + "\njobName: " + jobName +   "\naccountId: " + accountId + "\ntranscript: " + transcript;
		  Destination destination = new Destination().withToAddresses(new String[]{TO});
		  Content subject = new Content().withData(SUBJECT);
		  Content textBody = new Content().withData(BODY);
		  Body body = new Body().withText(textBody);
		  Message message = new Message().withSubject(subject).withBody(body);
		  SendEmailRequest request = new SendEmailRequest().withSource(FROM).withDestination(destination).withMessage(message);
		  client.sendEmail(request);
		 
	}
}