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
import java.util.Properties;
import java.util.HashMap;

/**
 * Utility file comprising constants shared across the project
 *
 * @author Vivekanand Ganapathy Nagarajan
 * @version 1.0 Feb 5th, 2017
 */
public class AWSUtil {



    public static final String tmp = "tmp/";

    public static final String flinkPath = "/efs/etl_pipeline/";

    public static final String homeDir = "user.dir";

    public static final String pipeline = "pipeline.log";

    public static final String awsKey = "awsKey";

    public static final String awsPassword = "awspassword";

    public static final String input_bucket = "input_bucket";

    public static final String output_bucket = "output_bucket";

    public static final String queryurl = "queryurl";

    public static final String redismaster = "redis_master";

    public static final String configProperties = "/config.properties";

    public static final String log4jProperties = "/log4j.properties";

    public static final String userConfig = "userconfig/";


    public static final String dagid = "dagid";

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
        String log4jConfigFile = System.getProperty(homeDir) + log4jProperties;
        File f = new File(log4jConfigFile);
        if (!f.exists()){
            log4jConfigFile = System.getProperty(homeDir) + flinkPath + log4jProperties;
        }
        PropertyConfigurator.configure(log4jConfigFile);
    }

    /**
     * Configure configProperties
     * @return
     * @throws IOException
     */
    public static HashMap<String, String> configProperties()  {
        HashMap<String, String> map = new HashMap<String, String>();

        try {
            Properties prop = new Properties();
            String configFile = System.getProperty(homeDir) + configProperties;
            File f = new File(configFile);
            if (!f.exists()){
                configFile = System.getProperty(homeDir) + AWSUtil.flinkPath + configProperties;
            }
            InputStream inputStream = new FileInputStream(configFile);
            prop.load(inputStream);

            map.put(awsKey, prop.getProperty(awsKey));
            map.put(awsPassword, prop.getProperty(awsPassword));
            map.put(input_bucket, prop.getProperty(input_bucket));
            map.put(output_bucket, prop.getProperty(output_bucket));
            map.put(queryurl, prop.getProperty(queryurl));
            map.put(redismaster, prop.getProperty(redismaster));
        }
        catch(IOException e){
            System.exit(-1);
        }
        return map;
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
            String projectPath = System.getProperty(AWSUtil.homeDir) + "/";
            final File homeDir = new File(projectPath + "tmp/");
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
