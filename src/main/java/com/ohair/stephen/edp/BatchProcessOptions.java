package com.ohair.stephen.edp;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * Options supported by {@link BatchProcessPipeline}.
 *
 * <p>
 * <b>Parameters:</b>
 * <ul>
 * <li>--input</li>
 * <li>--output</li>
 * <li>--stations</li>
 * <li>--years</li>
 * <li>--months</li>
 * <li>--days</li>
 * </ul>
 */
public interface BatchProcessOptions extends PipelineOptions {
	/*
	 * Default Options
	 */
	static final String DEFAULT_PROJECT_ID = "cloud-ai-weather-data-analyzer";
	static final String DEFAULT_GSOD_TABLE = "bigquery-public-data:noaa_gsod.gsod2017";
	static final String DEFAULT_STATIONS = "KYUX";
	static final String DEFAULT_YEAR = "2017";
	static final String DEFAULT_MONTHS = "12";
	static final String DEFAULT_DAYS = "23";

	/*
	 * Option: --input
	 */
	@Description("Table to read from, specified as " + "<project_id>:<dataset_id>.<table_id>")
	@Default.String(DEFAULT_GSOD_TABLE)
	String getInput();

	void setInput(String value);

	/*
	 * Option: --output
	 */
	@Description("Output directory")
	@Default.String("gs://" + DEFAULT_PROJECT_ID + "/" + "/nexrad/")
	String getOutput();

	void setOutput(String s);

	/*
	 * Option: --stations
	 */
	@Description("comma-separated radars to process")
	@Default.String(DEFAULT_STATIONS)
	String getRadars();

	void setRadars(String s);

	/*
	 * Option: --years
	 */
	@Description("comma-separated years to process")
	@Default.String(DEFAULT_YEAR)
	String getYears();

	void setYears(String s);

	/*
	 * Option: --months
	 */
	@Description("comma-separated months to process -- empty implies all")
	@Default.String(DEFAULT_MONTHS)
	String getMonths();

	void setMonths(String s);

	/*
	 * Option: --days
	 */
	@Description("comma-separated days to process -- empty implies all")
	@Default.String(DEFAULT_DAYS)
	String getDays();

	void setDays(String s);
}
