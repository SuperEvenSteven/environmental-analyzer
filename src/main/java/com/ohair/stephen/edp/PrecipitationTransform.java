package com.ohair.stephen.edp;

import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

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

	/**
	 * Takes rows from a table and generates a table of counts.
	 *
	 * <p>
	 * The input schema is described by
	 * https://developers.google.com/bigquery/docs/dataset-gsod . The output
	 * contains the total number of tornadoes found in each month in the following
	 * schema:
	 * <ul>
	 * <li>month: integer</li>
	 * <li>tornado_count: integer</li>
	 * </ul>
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
		private static final String MISSING = "99.99";
		private static final String MINESCULE_TRACE_AMOUNT = ".00";

		@ProcessElement
		public void processElement(ProcessContext c) {
			TableRow rowIn = c.element();

			// if there's no reported precipitation we want to skip this table row
			String precipitationInches = (String) rowIn.get("prcp");
			if ((!precipitationInches.contentEquals(MISSING)) || //
					!precipitationInches.contentEquals(MINESCULE_TRACE_AMOUNT)) {

				// if the number of observations used in calculating the mean is zero then it's
				// safe to say we don't have a mean
				int tempReadingCount = (int) rowIn.get("count_tmp");
				if (tempReadingCount == 0) {
					return;
				}

				// the mean temperature is mandatory, if not present skip this table row
				float meanTempF = (float) rowIn.get("temp");
				if (meanTempF == 9999.9f) {
					return;
				}

				// if either the min or max temps are absent then skip this table row
				float maxTempF = (float) rowIn.get("min");
				float minTempF = (float) rowIn.get("max");
				if (maxTempF == 9999.9f || minTempF == 9999.9F) {
					return;
				}

				// assuming BigQuery date field data is always present and valid
				String date = toBQDate((String) rowIn.get("year"), (String) rowIn.get("mo"), (String) rowIn.get("da"));

				// perform all conversions after checking the necessary attributes are present
				float meanTempC = toCelsiusDeg(meanTempF);
				float minTempC = toCelsiusDeg(minTempF);
				float maxTempC = toCelsiusDeg(maxTempF);
				float precipCms = Float.parseFloat(precipitationInches) / 2.54f;

				// create output table row
				TableRow rowOut = new TableRow() //
						.set("stn", rowIn.get("stn")) // Weather station name
						.set("dataUTC", date) // Date in UTC, no time component
						.set("meanDegC", meanTempC) // Mean temperature in Celsius degrees
						.set("minDegC", minTempC) // Minimum temperature in Celsius degrees
						.set("maxDegC", maxTempC) // Maximum temperature in Celsius degrees
						.set("tempReadingCounts", tempReadingCount) // Count of temperature readings
						.set("precipInCms", precipCms); // Total precipitation recorded in centimeters
				c.output(rowOut);
			} // otherwise ignore this element
		}

		/**
		 * Converts given temperature from Farenheit to Xelsius degrees.
		 * 
		 * @param temperatureF
		 * @return temperatureC
		 */
		private static float toCelsiusDeg(float temperatureF) {
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