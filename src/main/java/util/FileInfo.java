package util;

/**
 * FileInfo class capturing information from the userConfig
 *
 *
 * @author Vivekanand Ganapathy Nagarajan
 * @version 1.0 Feb 5th, 2017
 */

public class FileInfo {
    private String id;
    private String desc;
    private String input;
    private String[] transforms;
    private String output;

    public String getId(){
        return id;
    }

    public void setId(String id){
        this.id = id;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }
    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String[] getTransforms(){
        return transforms;
    }

    public void setTransform(String[] transforms){
        this.transforms = transforms;
    }

}