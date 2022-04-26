import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;

public class BeamDemoRunner {
    public static void main(String[] args) {
        //create a Pipeline object
        Pipeline pipeline=Pipeline.create();
        //creating a PCollection object which represents a distributed data set
        //and reading a text file (.txt)
        PCollection<String> output=pipeline.apply(
                TextIO.read().from("/home/prakhar/BeamPOC/input/sample-text-file-input.txt")
        );
        //converting the text file into a word document (.docx)
        output.apply(
                TextIO.write().to("/home/prakhar/BeamPOC/output/sample-text-file-output.docx")
                        //if wont use withNumShards , as I told, PCollection is a distibuted set
                        //it will output multiple files instead of 1 single file
                        .withNumShards(1)
                        //to generate a file with extension .docx
                        .withSuffix(".docx")
        );
        pipeline.run();
    }
}
