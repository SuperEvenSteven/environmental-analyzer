package com.ohair.stephen.edp;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * Options supported by {@link BatchProcessPipeline}.
 *
 * <p>
 * Inherits standard configuration options.
 */
public interface Options extends PipelineOptions {
	// Default to using a 1000 row subset of the public weather station table
	// publicdata:samples.gsod.
	static final String GSOD_TABLE = "bigquery-public-data:noaa_gsod.gsod2017";

	@Description("Table to read from, specified as " + "<project_id>:<dataset_id>.<table_id>")
	@Default.String(GSOD_TABLE)
	String getInput();

	void setInput(String value);

	static final String PRECIPITATION_FILTERED_GSOD = "cloud-ai-weather-data-analyzer:precipitation.filtered_gsod";

	@Description("BigQuery table to write to, specified as "
			+ "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
	@Default.String(PRECIPITATION_FILTERED_GSOD)
	@Validation.Required
	String getOutput();

	void setOutput(String value);
}
