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
	private final float precipitationCms;

	/**
	 * Constructor.
	 * 
	 * @param stationName
	 * @param dateUtc
	 * @param meanDegreesC
	 * @param minDegreesC
	 * @param maxDegreesC
	 * @param tempReadCounts
	 * @param precipitationCms
	 */
	public CombinedDataModel(String stationName, Date dateUtc, float meanDegreesC, float minDegreesC, float maxDegreesC,
			int tempReadCounts, float precipitationCms) {
		this.stationName = stationName;
		this.dateUtc = dateUtc;
		this.meanDegreesC = meanDegreesC;
		this.minDegreesC = minDegreesC;
		this.maxDegreesC = maxDegreesC;
		this.tempReadCounts = tempReadCounts;
		this.precipitationCms = precipitationCms;
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
		return precipitationCms;
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
		sb.append(precipitationCms);
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
}
