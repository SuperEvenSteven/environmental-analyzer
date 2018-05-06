package com.ohair.stephen.edp.transform;

import java.io.IOException;
import java.util.Date;
import java.util.Optional;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.annotations.VisibleForTesting;
import com.ohair.stephen.edp.model.GSODDataModel;
import com.ohair.stephen.edp.model.GSODDataModel.ModelBuilder;

/**
 * Defines the output Precipitation table schema and its transformations.
 * 
 * @author Stephen O'Hair
 *
 */
@SuppressWarnings("serial")
public final class GSODTransform extends PTransform<PCollection<TableRow>, PCollection<GSODDataModel>> {

	/**
	 * Takes rows from a table and returns a filtered rows that contain reported
	 * precipitation with temperature data.
	 *
	 * <p>
	 * The input schema is described by
	 * https://developers.google.com/bigquery/docs/dataset-gsod .
	 */

	@Override
	public PCollection<GSODDataModel> expand(PCollection<TableRow> rows) {
		// GSOD row... => combined data model with filtered precipitation data
		PCollection<GSODDataModel> model = rows.apply(ParDo.of(new SimplifyAndFilterPrecipitationFn()));
		return model;
	}

	/**
	 * Function that attempts to convert a GSOD row to a simplified and filtered
	 * GSODDataModel.
	 * 
	 * @author Stephen O'Hair
	 *
	 */
	static class SimplifyAndFilterPrecipitationFn extends DoFn<TableRow, GSODDataModel> {

		private static final Logger log = LoggerFactory.getLogger(SimplifyAndFilterPrecipitationFn.class);

		// As defined by the GSOD Table Schema
		private static final double MISSING_PRECIPITATION = 99.99f;
		private static final double MISSING_TEMP = 9999.9f;
		private static final double MINESCULE_TRACE_AMOUNT = .00f;
		private static final double CMS_PER_INCH = 2.54f;
		private static final int NO_TEMP_COUNTS = 0;

		private final Counter containPrecipitation = Metrics.counter(SimplifyAndFilterPrecipitationFn.class,
				"containPrecipitation");
		private final Counter missingPrecipitation = Metrics.counter(SimplifyAndFilterPrecipitationFn.class,
				"missingPrecipitation");
		private final Counter missingMeanTemps = Metrics.counter(SimplifyAndFilterPrecipitationFn.class,
				"missingMeanTempss");
		private final Counter missingTempCounts = Metrics.counter(SimplifyAndFilterPrecipitationFn.class,
				"missingTempCounts");
		private final Counter newModelElement = Metrics.counter(SimplifyAndFilterPrecipitationFn.class,
				"newModelElement");

		@ProcessElement
		public void processElement(ProcessContext c) throws IOException {
			TableRow rowIn = c.element();

			// if there's no reported precipitation we want to skip this table row
			double precipitationInches = (double) Optional.ofNullable(rowIn.get("prcp")).orElse(MISSING_PRECIPITATION);
			if ((precipitationInches != MISSING_PRECIPITATION) && //
					precipitationInches != MINESCULE_TRACE_AMOUNT) {
				containPrecipitation.inc();
				// If the number of observations used in calculating the mean is zero then it's
				// safe to say we don't have a mean, this data contains nulls.s
				// If null default to no temperature counts found and skip.

				int tempReadingCount = Integer
						.parseInt((String) Optional.ofNullable(rowIn.get("count_temp")).orElse(NO_TEMP_COUNTS));
				if (tempReadingCount == NO_TEMP_COUNTS) {
					missingTempCounts.inc();
					log.debug("skipping, missing count_tmp");
					return;
				}

				// the mean temperature is mandatory, if not present skip this table row
				double meanTempF = (double) Optional.ofNullable(rowIn.get("temp")).orElse(MISSING_TEMP);
				if (meanTempF == MISSING_TEMP) {
					missingMeanTemps.inc();
					log.debug("skipping, missing temp");
					return;
				}

				// if either the min or max temps are absent then skip this table row
				double maxTempF = (double) Optional.ofNullable(rowIn.get("min")).orElse(MISSING_TEMP);
				double minTempF = (double) Optional.ofNullable(rowIn.get("max")).orElse(MISSING_TEMP);
				if (maxTempF == MISSING_TEMP || minTempF == MISSING_TEMP) {
					log.debug("skipping, missing min max");
					return;
				}

				// assuming BigQuery date field data is always present and valid
				// String date = toBQDate((String) rowIn.get("year"), (String) rowIn.get("mo"),
				// (String) rowIn.get("da"));

				// perform all conversions after checking the necessary attributes are present
				double meanTempC = asCelsiusDeg(meanTempF);
				double minTempC = asCelsiusDeg(minTempF);
				double maxTempC = asCelsiusDeg(maxTempF);
				double precipCms = precipitationInches / CMS_PER_INCH;

				// create {@link CombinedDataModel} output object
				ModelBuilder builder = new GSODDataModel.ModelBuilder();
				builder.setStationName((String) rowIn.get("stn"));
				builder.setDateUtc(
						asDateUtc((String) rowIn.get("year"), (String) rowIn.get("mo"), (String) rowIn.get("da")));
				builder.setMeanDegreesC(meanTempC);
				builder.setMinDegreesC(minTempC);
				builder.setMaxDegreesC(maxTempC);
				builder.setTempReadCounts(tempReadingCount);
				builder.setPrecipitationCm(precipCms);
				newModelElement.inc();
				c.output(builder.build());

				// TableRow rowOut = new TableRow() //
				// .set("station_name", rowIn.get("stn")) // Weather station name
				// .set("date_utc", date) // Date in UTC, no time component
				// .set("mean_deg_c", meanTempC) // Mean temperature in Celsius degrees
				// .set("min_deg_c", minTempC) // Minimum temperature in Celsius degrees
				// .set("max_deg_c", maxTempC) // Maximum temperature in Celsius degrees
				// .set("temperature_reading_counts", tempReadingCount) // Count of temperature
				// readings
				// .set("percipitation_cms", precipCms); // Total precipitation recorded in
				// centimeters
				// c.output(rowOut);

				// log.debug("added precipitation row:" + rowOut.toPrettyString());
			} else {
				missingPrecipitation.inc();
				// otherwise ignore this element
				log.debug("skipping, missing precipitation measurement");
			}
		}

		/**
		 * Converts given temperature from Farenheit to Celsius degrees.
		 * 
		 * @param temperatureF
		 * @return temperatureC
		 */
		@VisibleForTesting
		private static double asCelsiusDeg(double temperatureF) {
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

		/**
		 * Returns
		 * 
		 * @param year
		 * @param month
		 * @param day
		 * @return java.util.Date
		 */
		@VisibleForTesting
		static Date asDateUtc(String year, String month, String day) {
			return null;
		}
	}
}