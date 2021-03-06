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

import java.io.File;
import java.util.HashMap;


/**
 * STEP3 of ETL Pipeline:
 * Perform Flink Transformations
 *
 * @author Vivekanand Ganapathy Nagarajan
 * @version 2.0 Feb 5th, 2017
 */
public class FlinkTransform {

    private static FileInfo fileInfo;
    private static Logger logger = Logger.getLogger(FlinkTransform.class);
    private static HashMap<String, String> values = null;


    public static void main(String[] args) throws Exception {
        AWSUtil.configureLog();
        values = AWSUtil.configProperties();
        if (args.length < 1){
            logger.error("dagid not specified");
	    System.exit(-1);
        }
        ArgumentParser parser = ArgumentParsers.newArgumentParser("dagid")
                .defaultHelp(true)
                .description("DagId");
        parser.addArgument("-d", "--dagid")
                .help("dagid");
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        String projectPath = System.getProperty(AWSUtil.homeDir) + AWSUtil.flinkPath;
        String dagid = ns.getString(AWSUtil.dagid);
        String filePathString = projectPath + AWSUtil.tmp + dagid + "/" + AWSUtil.pipeline;
        logger.info("filepathString " + filePathString);
        File f = new File(filePathString);
        if(f.isFile()) {
            String fileName = AWSUtil.readFromFile(filePathString);
            fileInfo = ReadYaml.readYaml(projectPath +  AWSUtil.userConfig + fileName);
            flinkExecute();
        } else{
            logger.warn("No entry in log");
            logger.warn("Failing job");
            System.exit(-1);
        }
    }

    /**
     * Execute flink transformations
     * @throws Exception
     */
    private static void flinkExecute() throws Exception{
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String input_bucket = values.get(AWSUtil.input_bucket);
        String path = input_bucket +"/"+fileInfo.getInput();
        setupFlinkTransforms(env, path,  fileInfo.getTransforms());
        env.execute();
    }

    /**
     * Set up flink transformations based on user input
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
        String output_bucket = values.get(AWSUtil.output_bucket);
        dataSet.writeAsText("s3a://"+ output_bucket +"/" + fileInfo.getOutput());
    }
}
