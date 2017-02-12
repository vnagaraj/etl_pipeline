package etl;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.apache.log4j.Logger;
import util.AWSUtil;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;



/**
 * STEP1 of ETL Pipeline:
 * Read from SQS Queue
 *
 * @author Vivekanand Ganapathy Nagarajan
 * @version 2.0 Feb 5th, 2017
 */

public class ReadMessageFromQueue {

    private static Logger logger = Logger.getLogger(ReadMessageFromQueue.class);

    public static void main(String[] args) throws IOException {
        if (args.length < 1){
            logger.error("output log not specified");
            System.exit(-1);
        }
        AWSUtil.createUserDir(args[0]);
        String path = AWSUtil.tmp + args[0]+"/";
        AWSUtil.writeToFile(path+AWSUtil.pipeline, readFromSQSQueue());

    }

    /**
     * Reading message from SQS queue after getting notified by S3 bucket
     *
     */
    public static String readFromSQSQueue() {
        HashMap<String, String> values = AWSUtil.configProperties();
        String queryUrl = values.get(AWSUtil.queryurl);
        AWSCredentials credentials = new BasicAWSCredentials(values.get(AWSUtil.awsKey),
                                        values.get(AWSUtil.awsPassword));
        AmazonSQS sqs = new AmazonSQSAsyncClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        sqs.setRegion(usWest2);
        AWSUtil.configureLog();
        ReceiveMessageRequest receiveMessageRequest = new
                ReceiveMessageRequest(queryUrl);
        List<Message> messages =
                sqs.receiveMessage(receiveMessageRequest).getMessages();
        String fileName = null;
        if (!messages.isEmpty()){
            Message message = messages.get(0);
            String m = message.getBody();
            fileName = AWSUtil.getFileName(m);
            logger.info("Got filename "+ fileName);
            deleteMessage(sqs, message, queryUrl);
        }else{
            logger.warn("filename set to None");
            logger.warn("fail the pipeline");
            System.exit(-1);
        }
        return fileName;
    }

    /**
     * Delete message after processing message from SQS queue
     * @param message
     */
    private static  void deleteMessage(AmazonSQS sqs, Message message, String queryUrl){
        sqs.deleteMessage(new DeleteMessageRequest()
                .withQueueUrl(queryUrl)
                .withReceiptHandle(message.getReceiptHandle()));
    }

}
