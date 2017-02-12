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
        AmazonS3 s3client = new AmazonS3Client(AWSUtil.credentials);
        if (AWSUtil.isBucketValid(s3client, AWSUtil.input_bucket)){
            for (File file: files) {
                s3client.putObject(new PutObjectRequest(AWSUtil.input_bucket, file.getName(), file));
            }
        }
        else{
            logger.error("Invalid bucket " + AWSUtil.input_bucket);
            System.exit(-1);
        }

    }

}
