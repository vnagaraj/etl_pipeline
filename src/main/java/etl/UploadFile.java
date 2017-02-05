package etl;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.log4j.Logger;
import util.AWSUtil;
import util.FileInfo;
import util.ReadYaml;
import java.io.File;

/**
 * STEP1 of ETL Pipeline:
 * Upload file to S3 Bucket
 *
 * @author Vivekanand Ganapathy Nagarajan
 * @version 1.0 Feb 5th, 2017
 */
public class UploadFile {
    private static Logger logger = Logger.getLogger(UploadFile.class);

    public static void main(String[] args) {
        AWSUtil.configureLog();
        if (args.length < 1){
            logger.error("user config not specified");
            System.exit(-1);
        }
        run(args[0]);
    }

    /**
     * Uploads a file from local user's directory/files/filename to S3 bucket
     *
     * @param fileName
     */
    public static void run(String fileName){
        FileInfo fileInfo = ReadYaml.readYaml(AWSUtil.filePath + "userconfig/" + fileName);
        AmazonS3 s3client = new AmazonS3Client(AWSUtil.credentials);
        if (AWSUtil.isBucketValid(s3client, AWSUtil.input_bucket)){
            File file = new File("files/" + fileInfo.getInput());
            s3client.putObject(new PutObjectRequest(AWSUtil.input_bucket, fileInfo.getInput(), file));
        }
        else{
            logger.error("Invalid bucket " + AWSUtil.input_bucket);
            System.exit(-1);
        }

    }

}
