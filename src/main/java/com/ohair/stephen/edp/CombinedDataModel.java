package com.ohair.stephen.edp;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

/**
 * Represents a combination of GSOD and NexRAD data.
 * 
 * @author Stephen O'Hair
 *
 */
@ThreadSafe
public class CombinedDataModel {

	/*
	 * Private member variables
	 */
	private final String stationName;
	private final Date dateUtc;
	private final float meanDegreesC;
	private final float minDegreesC;
	private final float maxDegreesC;
	private final int tempReadCounts;
	private final float precipitationCm;

	/**
	 * Constructor.
	 * 
	 * @param ModelBuilder
	 */
	public CombinedDataModel(ModelBuilder builder) {
		this.stationName = builder.stationName;
		this.dateUtc = builder.dateUtc;
		this.meanDegreesC = builder.meanDegreesC;
		this.minDegreesC = builder.minDegreesC;
		this.maxDegreesC = builder.maxDegreesC;
		this.tempReadCounts = builder.tempReadCounts;
		this.precipitationCm = builder.precipitationCm;
	}

	/*
	 * Public accessor methods
	 */

	public String getStationName() {
		return stationName;
	}

	public Date getDateUtc() {
		return dateUtc;
	}

	public float getMeanDegreesC() {
		return meanDegreesC;
	}

	public float getMinDegreesC() {
		return minDegreesC;
	}

	public float getMaxDegreesC() {
		return maxDegreesC;
	}

	public int getTempReadCounts() {
		return tempReadCounts;
	}

	public float getPrecipitationCms() {
		return precipitationCm;
	}

	/**
	 * @return csv representation of a {@link CombinedDataModel}
	 */
	public String toCsv() {
		StringBuilder sb = new StringBuilder();
		sb.append(stationName).append(",");
		sb.append(dateUtc.getTime()).append(",");
		sb.append(meanDegreesC).append(",");
		sb.append(minDegreesC).append(",");
		sb.append(maxDegreesC).append(",");
		sb.append(tempReadCounts).append(",");
		sb.append(precipitationCm);
		return sb.toString();
	}

	/**
	 * Returns this as a BigQuery TableSchema should it need to be persisted there.
	 * 
	 * @return TableSchema
	 */
	public static TableSchema tableSchema() {
		// Build the table schema for the output table.
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("station_name").setType("STRING"));
		fields.add(new TableFieldSchema().setName("date_utc").setType("DATE"));
		fields.add(new TableFieldSchema().setName("mean_deg_c").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("min_deg_c").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("max_deg_c").setType("FLOAT"));
		fields.add(new TableFieldSchema().setName("temperature_reading_counts").setType("INTEGER"));
		fields.add(new TableFieldSchema().setName("precipitation").setType("FLOAT"));
		TableSchema schema = new TableSchema().setFields(fields);
		return schema;
	}

	/*
	 * Builder patter as per: https://stackoverflow.com/a/20940894/370003
	 */
	private static class ModelBuilder {
		/*
		 * Private member variables
		 */
		private String stationName;
		private Date dateUtc;
		private float meanDegreesC;
		private float minDegreesC;
		private float maxDegreesC;
		private int tempReadCounts;
		private float precipitationCm;

		/**
		 * Constructor.
		 * 
		 */
		public ModelBuilder() {
		}

		/*
		 * Public mutator methods
		 */

		public ModelBuilder setStationName(String stationName) {
			this.stationName = stationName;
			return this;
		}

		public ModelBuilder setDateUtc(Date dateUtc) {
			this.dateUtc = dateUtc;
			return this;
		}

		public ModelBuilder setMeanDegreesC(float meanDegreesC) {
			this.meanDegreesC = meanDegreesC;
			return this;
		}

		public ModelBuilder setMinDegreesC(float minDegreesC) {
			this.minDegreesC = minDegreesC;
			return this;
		}

		public ModelBuilder setMaxDegreesC(float maxDegreesC) {
			this.maxDegreesC = maxDegreesC;
			return this;
		}

		public ModelBuilder setTempReadCounts(int tempReadCounts) {
			this.tempReadCounts = tempReadCounts;
			return this;
		}

		public ModelBuilder setPrecipitationCm(int precipitationCm) {
			this.precipitationCm = precipitationCm;
			return this;
		}

		/**
		 * Builder build.
		 * 
		 * @return {@link CombinedDataModel}
		 */
		public CombinedDataModel build() {
			return (new CombinedDataModel(this));
		}
	}
}
