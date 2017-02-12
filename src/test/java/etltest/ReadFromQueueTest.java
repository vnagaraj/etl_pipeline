package etltest;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import etl.ReadMessageFromQueue;
import etl.UploadFiles;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import util.AWSUtil;

/**
 * Unit Test for
 * Reading from SQS queue
 *
 * @author Vivekanand Ganapathy Nagarajan
 * @version 1.0 Feb 5th, 2017
 */
public class ReadFromQueueTest extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public ReadFromQueueTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( ReadFromQueueTest.class );
    }

    public void setUp(){
        UploadFiles.run("userconfig");
    }

    /*
     * unit test to verify reading from SQS queue
     *
     */
    public void testReadFromQueue()
    {
        ReadMessageFromQueue.readFromSQSQueue();
    }

    public void tearDown()
    {
        AmazonS3 s3client = new AmazonS3Client(AWSUtil.credentials);
        s3client.deleteObject(new DeleteObjectRequest(AWSUtil.input_bucket, "destfile_1.txt"));
    }


}

