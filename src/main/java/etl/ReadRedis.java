package etl;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import util.AWSUtil;
import java.io.*;
import java.util.HashMap;


/**
 * STEP2 of ETL Pipeline:
 * Read from Redis(key:fileName, value:userInfo)
 *
 * @author Vivekanand Ganapathy Nagarajan
 * @version 1.0 Feb 5th, 2017
 */
public class ReadRedis {

    private static Logger logger = Logger.getLogger(ReadRedis.class);

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        AWSUtil.configureLog();
        if (args.length < 1){
            logger.error("dagid not specified");
            System.exit(-1);
        }
        HashMap<String, String> values = AWSUtil.configProperties();
        String project_location = values.get("project_location");
        String filePathString = project_location + AWSUtil.tmp + args[0]+"/" + AWSUtil.pipeline;
        outputUser(filePathString);

    }

    /**
     *
     * Check if fileName is present in Redis,
     * if present deletes fileName and outputs key
     * else fails the stage
     *
     * @param filePathString
     * @throws IOException
     */
    private static void outputUser(String filePathString) throws IOException{
        File f = new File(filePathString);
        if(f.isFile()) {
            String fileName = AWSUtil.readFromFile(filePathString);
            //check in redis for fileName
            HashMap<String, String> values = AWSUtil.configProperties();
            JedisPool pool = new JedisPool(new JedisPoolConfig(), values.get(AWSUtil.redismaster));
            Jedis jedis = pool.getResource();
            if (jedis.exists(fileName)){
                String val = jedis.get(fileName);
                logger.info("FileName exists in Redis");
                logger.info("Can process message down the pipeline");
                AWSUtil.writeToFile(filePathString, val);
                jedis.del(fileName);
                logger.info("Deleted key " + fileName + " from redis");
            } else{
                logger.warn("Duplicate Message");
                logger.warn("Failing job");
                System.exit(-1);
            }
        } else{
            logger.warn("No message from queue");
            logger.warn("Failing job");
            System.exit(-1);
        }
    }

}
