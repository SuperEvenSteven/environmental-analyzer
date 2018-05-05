package com.ohair.stephen.edp;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;

/**
 * @author Stephen O'Hair
 * @see based from <a href=
 *      "https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/BigQueryTornadoes.java">examples</a>
 *      found on the Apache Beam GitHub repository.
 */
public class Main {

	/**
	 */
	public interface Options extends PipelineOptions {

		@Description("Path to the data file(s) containing game data.")
		// The default maps to two large Google Cloud Storage files (each ~12GB) holding
		// two subsequent
		// day's worth (roughly) of data.
		@Default.String("gs://apache-beam-samples/game/gaming_data*.csv")
		String getInput();

		void setInput(String value);

		// Set this required option to specify where to write the output.
		@Description("Path of the file to write to.")
		@Validation.Required
		String getOutput();

		void setOutput(String value);
	}

	public static void main(String[] args) throws Exception {
		// Begin constructing a pipeline configured by commandline flags.
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);

		// Run query from BigQuery
		// Extract attributes as defined in data schema
		// Download all NexRad data from BigData
		// Extract radar reflectivity data
		// Create output files

		// Read events from a text file and parse them.
		// pipeline.apply(TextIO.read().from(options.getInput())).apply("ParseGameEvent",
		// ParDo.of(new ParseEventFn()))
		// Extract and sum username/score pairs from the event data.
		// .apply("ExtractUserScore", new ExtractAndSumScore("user"))
		// .apply("WriteUserScoreSums", new WriteToText<>(options.getOutput(),
		// configureOutput(), false));

		// Run the batch pipeline.
		pipeline.run().waitUntilFinish();
	}
}
