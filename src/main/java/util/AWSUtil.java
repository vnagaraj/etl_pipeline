package util;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.util.HashMap;

/**
 * Utility file comprising constants shared across the project
 *
 * @author Vivekanand Ganapathy Nagarajan
 * @version 1.0 Feb 5th, 2017
 */
public class AWSUtil {

    public static final AWSCredentials credentials = new BasicAWSCredentials(
            "*****",
            "*****");

    public static final String queueUrl = "*****";

    public static final String input_bucket = "*****";

    public static final String output_bucket = "*****";

    public static final String redis_master = "*****";

    public static final String filePath = System.getProperty("user.dir")+ "/";

    public static final String tmp = filePath + "tmp/";

    public static final String pipeline = "pipeline.log";

    private static Logger logger = Logger.getLogger(AWSUtil.class);

    public static boolean isBucketValid(AmazonS3 s3client, String bucketName){
        for (Bucket bucket : s3client.listBuckets()) {
            if (bucketName.equals(bucket.getName())){
                return true;
            }
        }
        return false;
    }

    /**
     * Configure Log4J properties
     */
    public static void configureLog(){
        String log4jConfigFile = System.getProperty("user.dir")
                + File.separator + "log4j.properties";
        PropertyConfigurator.configure(log4jConfigFile);
    }

    public static String getFileName(String message) {
        String result = null;
        try {
            // convert JSON string to Map
            HashMap<String, String> hashMap = new HashMap<String, String>();
            JSONObject json = new JSONObject(message);
            JSONArray records = (JSONArray) json.get("Records");
            JSONObject records0 = (JSONObject) records.get(0);
            JSONObject s3 = (JSONObject) records0.get("s3");
            JSONObject object = (JSONObject) s3.get("object");
            result =  (String) object.get("key");
        }
        catch (JSONException e){
            logger.error("Failure to parse sqs message " + message);
            System.exit(-1);
        }
        return result;
    }

    public static void writeToFile(String path, String message) throws IOException {
        /*
        if (message == null){
            logger.warn("No message to write");
            return;
        }*/
        BufferedWriter writer = null;
        try{
            File file = new File(path);
            writer = new BufferedWriter(new FileWriter(file));
            writer.write(message);
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    public static String readFromFile(String path) throws IOException {
        BufferedReader Buff = new BufferedReader(new FileReader(path));
        String text = Buff.readLine();
        logger.info("Got fileName " + text);
        return text;
    }

    public static void createUserDir(final String dirName)  {
        try {
            final File homeDir = new File(AWSUtil.filePath + "tmp/");
            final File dir = new File(homeDir, dirName);
            if (!dir.exists() && !dir.mkdirs()) {
                logger.warn("Directory exists " + dirName);
                return;
            }
        }
        catch (Exception e){
            logger.error("Failure to create directory " + dirName);
            System.exit(-1);
        }
    }

}
