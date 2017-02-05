package util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.log4j.Logger;

import java.io.File;


/**
 * ReadYaml class to capture FileInfo object
 * <format>
 *  id: pipeline_1
 *  desc: pipleline_desc
 *  input:  input_file
 *  output: output_file
 *  transforms:
 *       - transform1
 *       - transform2
 *</format>
 *
 * @author Vivekanand Ganapathy Nagarajan
 * @version 1.0 Feb 5th, 2017
 */
public class ReadYaml {

    private static Logger logger = Logger.getLogger(ReadYaml.class);

    /**
     * Utility method to parse yaml file as FileInfo object
     * @param fileName
     * @return FileInfo
     */
    public  static FileInfo readYaml(String fileName) {
        AWSUtil.configureLog();
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        FileInfo fileInfo = null;
        try {
            fileInfo = mapper.readValue(new File(fileName), FileInfo.class);
            logger.info(ReflectionToStringBuilder.toString(fileInfo,ToStringStyle.MULTI_LINE_STYLE));

        } catch (Exception e) {
            logger.error("Failure to parse yaml ");
            System.exit(-1);
        }
        return fileInfo;
    }

}
