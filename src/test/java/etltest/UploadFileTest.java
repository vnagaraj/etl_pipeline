package etltest;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import etl.UploadFile;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import util.AWSUtil;


/**
 * Unit Test for
 * Upload file to S3 Bucket
 *
 * @author Vivekanand Ganapathy Nagarajan
 * @version 1.0 Feb 5th, 2017
 */
public class UploadFileTest
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public UploadFileTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( UploadFileTest.class );
    }

    /*
     * unit test to verify upload of file to S3 bucket
     */
    public void testUploadFile()
    {
        UploadFile.run("user1.yaml");
        String expFileName = "destfile_1.txt";
        boolean found = false;
        AmazonS3 s3 = new AmazonS3Client(AWSUtil.credentials);
        ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                .withBucketName(AWSUtil.input_bucket));
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            if (objectSummary.getKey().equals(expFileName)){
                found = true;
                break;
            }
        }
        assertTrue("Failure to verify file upload", found);

    }
}
