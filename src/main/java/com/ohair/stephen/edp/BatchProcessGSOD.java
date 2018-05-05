package com.ohair.stephen.edp;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

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
public class BatchProcessGSOD {

	/*
	 * Static utility methods used for data transforms and pipeline execution.
	 */
	private static void runBigQueryGSOD(Options options) {
		Pipeline p = Pipeline.create(options);

		// Build the table schema for the output table.
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("month").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("tornado_count").setType("INTEGER"));
		TableSchema schema = new TableSchema().setFields(fields);

		p.apply(BigQueryIO.readTableRows() //
				.from(options.getInput())) //
				.apply(new PrecipitationTransform()) //
				.apply(BigQueryIO.writeTableRows() //
						.to(options.getOutput()).withSchema(schema)//
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
