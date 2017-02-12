package etl;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.log4j.Logger;
import util.AWSUtil;
import util.FileInfo;
import util.ReadYaml;

import java.io.File;
import java.util.HashMap;

/**
 * Preprocessing step of ETL Pipeline:
 * Uploads files to S3 bucket
 *
 * @author Vivekanand Ganapathy Nagarajan
 * @version 2.0 Feb 5th, 2017
 */
public class UploadFiles {
    private static Logger logger = Logger.getLogger(UploadFiles.class);

    public static void main(String[] args) {
        AWSUtil.configureLog();
        if (args.length < 1){
            logger.error("file location not specified");
            System.exit(-1);
        }
        run(args[0]);
    }

    /**
     * Uploads batch of files in S3 bucket, mimics user's request
     *
     * @param fileDir
     */
    public static void run(String fileDir){
        File[] files = new File(fileDir).listFiles();
        if (files == null){
            return;
        }
        HashMap<String, String> values = AWSUtil.configProperties();
        String awsKey = values.get(AWSUtil.awsKey);
        String awsPassword = values.get(AWSUtil.awsPassword);
        AWSCredentials credentials = new BasicAWSCredentials(awsKey, awsPassword);
        AmazonS3 s3client = new AmazonS3Client(credentials);
        String inputBucket = values.get(AWSUtil.input_bucket);
        if (AWSUtil.isBucketValid(s3client, inputBucket)){
            for (File file: files) {
                s3client.putObject(new PutObjectRequest(inputBucket, file.getName(), file));
            }
        }
        else{
            logger.error("Invalid bucket " + inputBucket);
            System.exit(-1);
        }

    }

}
