package com.ohair.stephen.edp;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * Options supported by {@link BatchProcessGSOD}.
 *
 * <p>
 * Inherits standard configuration options.
 */
public interface Options extends PipelineOptions {
	// Default to using a 1000 row subset of the public weather station table
	// publicdata:samples.gsod.
	static final String WEATHER_SAMPLES_TABLE = "clouddataflow-readonly:samples.weather_stations";

	@Description("Table to read from, specified as " + "<project_id>:<dataset_id>.<table_id>")
	@Default.String(WEATHER_SAMPLES_TABLE)
	String getInput();

	void setInput(String value);

	@Description("BigQuery table to write to, specified as "
			+ "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
	@Validation.Required
	String getOutput();

	void setOutput(String value);
}
