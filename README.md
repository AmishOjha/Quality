import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class DataflowExample {
    public static void main(String[] args) {
        // Create the pipeline options
        PipelineOptions options = PipelineOptionsFactory.create();

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);    

        // Read data from a text file
        pipeline
                .apply("ReadFromText", TextIO.read().from("gs://your-bucket/input.txt"))
                .apply(
                        "ProcessData",
                        ParDo.of(
                                new DoFn<String, String>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext c) {
                                        // Process each element
                                        String input = c.element();
                                        // Perform some transformation or processing
                                        String output = "Processed: " + input;
                                        // Output the processed data
                                        c.output(output);
                                    }
                                }))
                .apply("WriteToText", TextIO.write().to("gs://your-bucket/output.txt"));

        // Run the pipeline
        pipeline.run();
    }
} 
