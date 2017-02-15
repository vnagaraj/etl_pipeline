package etl;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
//import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import util.AWSUtil;

import java.util.HashMap;

/**
 * Cleanup script:
 * Deletes files in input bucket, output bucket and purges queue
 *
 * @author Vivekanand Ganapathy Nagarajan
 * @version 1.0 Feb 14th, 2017
 */
public class CleanupFiles {

    public static void main(String[] args){
        HashMap<String, String> values = AWSUtil.configProperties();
        String awsKey = values.get(AWSUtil.awsKey);
        String awsPassword = values.get(AWSUtil.awsPassword);
        AWSCredentials credentials = new BasicAWSCredentials(awsKey, awsPassword);
        String inputBucket = values.get(AWSUtil.input_bucket);
        String outputBucket = values.get(AWSUtil.output_bucket);
        String queryurl = values.get(AWSUtil.queryurl);
        AmazonS3 s3client = new AmazonS3Client(credentials);
        AmazonSQSClient sqsclient = new AmazonSQSClient(credentials);
        deleteFilesInInput(s3client, inputBucket);
        deleteFilesInOutput(s3client, outputBucket);
        //purgeQueue(sqsclient, queryurl);
    }

    public static void deleteFilesInInput(AmazonS3 s3client, String inputBucket){
        String fileName = "inputfile_";
        for (int i=1; i < 40; i++) {
            String suffix = String.valueOf(i) + ".txt";
            s3client.deleteObject(new DeleteObjectRequest(inputBucket, fileName + suffix));
        }
    }

    public static void deleteFilesInOutput(AmazonS3 s3client, String ouputBucket){
        String folderName = "output_";
        for (int i=1; i < 40; i++) {
            String suffix = String.valueOf(i);
            deleteObjectsInFolder(ouputBucket, folderName + suffix, s3client );
        }
    }
    /*
    public static void purgeQueue(AmazonSQSClient sqsclient, String queryurl){
        sqsclient.purgeQueue(new PurgeQueueRequest().withQueueUrl(queryurl));
    }
    */

    private static void deleteObjectsInFolder(String bucketName, String folderPath, AmazonS3 s3client) {
        for (S3ObjectSummary file : s3client.listObjects(bucketName, folderPath).getObjectSummaries()){
            s3client.deleteObject(bucketName, file.getKey());
        }
    }


}
