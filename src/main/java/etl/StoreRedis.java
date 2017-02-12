package etl;

import org.apache.log4j.Logger;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import util.AWSUtil;
import util.ReadYaml;
import util.FileInfo;
import redis.clients.jedis.Jedis;

import java.io.File;
import java.util.HashMap;


/**
 * Preprocessing step of ETL Pipeline:
 * Store in Redis(key:fileName, value:userInfo)
 *
 * @author Vivekanand Ganapathy Nagarajan
 * @version 1.0 Feb 5th, 2017
 */
public class StoreRedis {
    private static Logger logger = Logger.getLogger(StoreRedis.class);

    public static void main(String[] args) {
        AWSUtil.configureLog();
        if (args.length < 1){
            logger.error("userconfig dir not specified");
            System.exit(-1);
        }
        run(args[0]);
    }

    public static void run(String fileDir){
        File[] files = new File(fileDir).listFiles();
        if (files == null){
            logger.error("Listing of files failed");
            System.exit(-1);
        }
        logger.info("Storing in Redis");
        HashMap<String, String> values = AWSUtil.configProperties();
        JedisPool pool = new JedisPool(new JedisPoolConfig(), values.get(AWSUtil.redismaster));
        Jedis jedis = pool.getResource();
        for (File file: files) {
            if (!file.getName().endsWith("yaml")){
                continue;
            }
            //Path p = Paths.get(file.getName());
            FileInfo fileInfo =  ReadYaml.readYaml(file.getPath());
            String fileName = fileInfo.getInput();
            logger.info("Storing key "+ fileName + " value " + file.getName());
            //store data in redis list
            jedis.set(fileName, file.getName());
        }

    }
}
