package com.ohair.stephen.edp;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * Simple class that extracts GSOD records from Google BigQuery and converts
 * them to CSV. Runs in batch since the rate at which the data is updated
 * doesn't warrant streaming.
 * 
 * Based from the BigQueryTornado example found on the Apache Beam GitHub
 * repository <a href=
 * "https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/BigQueryTornadoes.java">source</a>.
 * 
 * @author Stephen O'Hair
 * @see <a href=
 *      "https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/BigQueryTornadoes.java">attribution</a>
 */
public class BatchProcessPipeline {

	/*
	 * Static utility methods used for data transforms and pipeline execution.
	 */
	private static void runBigQueryGSOD(Options options) {
		Pipeline p = Pipeline.create(options);

		p.apply(BigQueryIO.readTableRows() //
				.from(options.getInput())) //
				.apply(new PrecipitationTransform()) //
				.apply(BigQueryIO.writeTableRows() //
						.to(options.getOutput()).withSchema(CombinedDataModel.tableSchema())//
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED) //
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

		p.run().waitUntilFinish();
	}

	/*
	 * Main entry point.
	 */
	public static void main(String[] args) throws Exception {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		runBigQueryGSOD(options);
	}
}
