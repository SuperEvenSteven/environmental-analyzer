package com.ohair.stephen.edp;

import java.io.IOException;
import java.util.Optional;

import org.apache.beam.examples.DebuggingWordCount.FilterTextFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;

/**
 * Defines the output Precipitation table schema and its transformations.
 * 
 * @author Stephen O'Hair
 *
 */
public class PrecipitationTransform extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {

	/**
	 * Generated UUID
	 */
	private static final long serialVersionUID = -2055027164272213259L;

	private static final Logger logger = LoggerFactory.getLogger(PrecipitationTransform.class);

	/**
	 * Takes rows from a table and returns a filtered rows that contain reported
	 * precipitation with temperature data.
	 *
	 * <p>
	 * The input schema is described by
	 * https://developers.google.com/bigquery/docs/dataset-gsod .
	 */

	@Override
	public PCollection<TableRow> expand(PCollection<TableRow> rows) {

		// GSOD row... => filtered precipitation row
		PCollection<TableRow> gsod = rows.apply(ParDo.of(new SimplifyAndFilterPrecipitationFn()));

		return gsod;
	}

	/**
	 * Function that attempts to convert a GSOD row to a simplified and filtered
	 * precipitation row.
	 * 
	 * @author Stephen O'Hair
	 *
	 */
	static class SimplifyAndFilterPrecipitationFn extends DoFn<TableRow, TableRow> {

		/**
		 * Generated UUID
		 */
		private static final long serialVersionUID = -2702511629178536470L;

		// As defined by the GSOD Table Schema
		private static final double MISSING_PRECIPITATION = 99.99f;
		private static final double MISSING_TEMP = 9999.9f;
		private static final double MINESCULE_TRACE_AMOUNT = .00f;
		private static final double CMS_PER_INCH = 2.54f;
		private static final int NO_TEMP_COUNTS = 0;

		@ProcessElement
		public void processElement(ProcessContext c) throws IOException {
			TableRow rowIn = c.element();

			// if there's no reported precipitation we want to skip this table row
			double precipitationInches = (double) Optional.ofNullable(rowIn.get("prcp")).orElse(MISSING_PRECIPITATION);
			if ((precipitationInches != MISSING_PRECIPITATION) || //
					precipitationInches != MINESCULE_TRACE_AMOUNT) {

				// If the number of observations used in calculating the mean is zero then it's
				// safe to say we don't have a mean, this data contains nulls.
				// If null default to no temperature counts found and skip.

				int tempReadingCount = (int) Optional.ofNullable(rowIn.get("count_tmp")).orElse(NO_TEMP_COUNTS);
				if (tempReadingCount == NO_TEMP_COUNTS) {
					return;
				}

				// the mean temperature is mandatory, if not present skip this table row
				double meanTempF = (double) Optional.ofNullable(rowIn.get("temp")).orElse(MISSING_TEMP);
				if (meanTempF == MISSING_TEMP) {
					return;
				}

				// if either the min or max temps are absent then skip this table row
				double maxTempF = (double) Optional.ofNullable(rowIn.get("min")).orElse(MISSING_TEMP);
				double minTempF = (double) Optional.ofNullable(rowIn.get("max")).orElse(MISSING_TEMP);
				if (maxTempF == MISSING_TEMP || minTempF == MISSING_TEMP) {
					return;
				}

				// assuming BigQuery date field data is always present and valid
				String date = toBQDate((String) rowIn.get("year"), (String) rowIn.get("mo"), (String) rowIn.get("da"));

				// perform all conversions after checking the necessary attributes are present
				double meanTempC = toCelsiusDeg(meanTempF);
				double minTempC = toCelsiusDeg(minTempF);
				double maxTempC = toCelsiusDeg(maxTempF);
				double precipCms = precipitationInches / CMS_PER_INCH;

				// create output table row
				TableRow rowOut = new TableRow() //
						.set("station_name", rowIn.get("stn")) // Weather station name
						.set("date_utc", date) // Date in UTC, no time component
						.set("mean_deg_c", meanTempC) // Mean temperature in Celsius degrees
						.set("min_deg_c", minTempC) // Minimum temperature in Celsius degrees
						.set("max_deg_c", maxTempC) // Maximum temperature in Celsius degrees
						.set("temperature_reading_counts", tempReadingCount) // Count of temperature readings
						.set("percipitation_cms", precipCms); // Total precipitation recorded in centimeters
				c.output(rowOut);
				logger.info("added precipitation row:" + rowOut.toPrettyString());
			} // otherwise ignore this element
		}

		/**
		 * Converts given temperature from Farenheit to Xelsius degrees.
		 * 
		 * @param temperatureF
		 * @return temperatureC
		 */
		private static double toCelsiusDeg(double temperatureF) {
			return ((temperatureF - 32) * 5) / 9;
		}

		/**
		 * Returns a BigQuery standard sql date in the canonical format
		 * 'YYYY-[M]M-[D]D'.
		 * 
		 * @param year
		 * @param month
		 * @param day
		 * @return
		 */
		private static String toBQDate(String year, String month, String day) {
			StringBuffer sb = new StringBuffer();
			sb.append(year).append("-").append(month).append("-").append(day);
			return sb.toString();
		}
	}
}