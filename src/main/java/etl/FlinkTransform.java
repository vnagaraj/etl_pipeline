package etl;

import org.apache.log4j.Logger;
import util.AWSUtil;
import util.FileInfo;
import util.ReadYaml;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;


/**
 * STEP3 of ETL Pipeline:
 * Perform Flink Transformations
 *
 * @author Vivekanand Ganapathy Nagarajan
 * @version 1.0 Feb 5th, 2017
 */
public class FlinkTransform {

    private static FileInfo fileInfo;
    private static Logger logger = Logger.getLogger(FlinkTransform.class);


    public static void main(String[] args) throws Exception {
        if (args.length < 1){
            logger.error("user config not specified");
            System.exit(-1);
        }
        ArgumentParser parser = ArgumentParsers.newArgumentParser("UserConfig")
                .defaultHelp(true)
                .description("User config.");
        parser.addArgument("-u", "--userConfig")
                .help("user config file");
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        String fileName = ns.getString("userConfig");
        fileInfo = ReadYaml.readYaml(fileName);
        flinkExecute();


    }

    /**
     * Execute flink transformations
     * @throws java.lang.Exception
     */
    private static void flinkExecute() throws java.lang.Exception{
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String path = AWSUtil.input_bucket +"/"+fileInfo.getInput();
        setupFlinkTransforms(env, path,  fileInfo.getTransforms());
        env.execute();
    }

    /**
     * Set up flink transformations
     * @param env remote environment
     * @param path input path in s3 bucket
     * @param transforms user specified transformations
     */
    private static void setupFlinkTransforms(ExecutionEnvironment env, String path, String[] transforms){
        DataSource<String> source = null;
        DataSet<String> dataSet = null;
        for (String transform: transforms){
            if (transform.equals("textInputFormat")){
                source = env.readTextFile("s3a://"+path);
            }
            else if (transform.contains("filterRecord")){
                if (transform.length() == 1){
                    logger.error("Invalid transformation " + transform);
                    System.exit(-1);
                }
                String filterWord =transform.split("\\s+")[1];
                logger.info("filterWord " + filterWord);
                if (source == null){
                    logger.error("inputFormat not set");
                    System.exit(-1);
                }
                dataSet = source.filter(line -> line.contains(filterWord));
            }
            else{
                logger.error("Invalid transformation " + transform);
                System.exit(-1);
            }
        }
        if (dataSet == null){
           logger.error("outputFormat not set ");
            System.exit(-1);
        }
        dataSet.writeAsText("s3a://"+ AWSUtil.output_bucket +"/" + fileInfo.getOutput());
    }
}
