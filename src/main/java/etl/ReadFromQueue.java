package etl;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import util.AWSUtil;
import util.FileInfo;
import util.ReadYaml;
import org.apache.log4j.Logger;
import java.util.List;

/**
 * STEP2 of ETL Pipeline:
 * Read from SQS Queue
 *
 * @author Vivekanand Ganapathy Nagarajan
 * @version 1.0 Feb 5th, 2017
 */

public class ReadFromQueue {

    private static FileInfo fileInfo;
    private static Logger logger = Logger.getLogger(ReadFromQueue.class);
    private static AmazonSQS sqs = new AmazonSQSAsyncClient(AWSUtil.credentials);


    public static void main(String[] args)  {
        AWSUtil.configureLog();
        if (args.length < 1){
            logger.error("user config not specified");
            System.exit(-1);
        }
        run(args[0]);
    }

    /**
     * Client reading message from SQS queue after getting notified by S3 bucket
     * Retries until message matched expected fileName in S3 bucket
     */
    private static void readFromSQSQueue() {
        ReceiveMessageRequest receiveMessageRequest = new
                ReceiveMessageRequest(AWSUtil.queueUrl);
        List<Message> messages =
                sqs.receiveMessage(receiveMessageRequest).getMessages();
        boolean foundMessage = false;
        while (messages.size() != 0) {
            for (Message message : messages) {
                String m = message.getBody();
                String fileName = AWSUtil.getFileName(m);
                logger.info("Got fileName " + fileName);
                if (fileName.equals(fileInfo.getInput())) {
                    foundMessage = true;
                    logger.info("Found message " + message);
                    deleteMessage(message);
                    break;
                }
            }
            if (foundMessage){
                break;
            }
            messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        }
        if (!foundMessage){
            readFromSQSQueue();
        }
    }

    public static void run(String yaml) {
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        sqs.setRegion(usWest2);
        fileInfo = ReadYaml.readYaml(AWSUtil.filePath + "userconfig/" + yaml);
        readFromSQSQueue();

    }

    /**
     * Delete message after processing message from SQS queue
     * @param message
     */
    private static  void deleteMessage(Message message){
        sqs.deleteMessage(new DeleteMessageRequest()
                .withQueueUrl(AWSUtil.queueUrl)
                .withReceiptHandle(message.getReceiptHandle()));
    }

}
