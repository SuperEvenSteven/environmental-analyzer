package com.ohair.stephen.edp.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

/**
 * Represents the NexRAD Level II data model.
 * 
 * @author Stephen O'Hair
 *
 */
@ThreadSafe
public final class NexRadDataModel {

	/*
	 * Private member variables
	 */
	private final String stationName;
	private final Date dateUtc;

	/**
	 * Constructor.
	 * 
	 * @param builder - object builder
	 */
	public NexRadDataModel(ModelBuilder builder) {
		this.stationName = builder.stationName;
		this.dateUtc = builder.dateUtc;
	}

	/**
	 * @return csv representation of a {@link NexRadDataModel}
	 */
	public String toCsv() {
		StringBuilder sb = new StringBuilder();
		sb.append(stationName).append(",");
		sb.append(dateUtc.getTime()).append(",");
		return sb.toString();
	}

	/**
	 * Returns this as a BigQuery {@link TableSchema} should it need to be persisted
	 * there.
	 * 
	 * @return TableSchema
	 */
	public static TableSchema tableSchema() {
		// Build the table schema for the output table.
		List<TableFieldSchema> fields = new ArrayList<>();
		fields.add(new TableFieldSchema().setName("station_name").setType("STRING"));
		fields.add(new TableFieldSchema().setName("date_utc").setType("DATE"));
		TableSchema schema = new TableSchema().setFields(fields);
		return schema;
	}

	/*
	 * Builder pattern as per: https://stackoverflow.com/a/20940894/370003
	 */
	public static class ModelBuilder {
		/*
		 * Private member variables
		 */
		private String stationName;
		private Date dateUtc;

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

		/**
		 * Builder build.
		 * 
		 * @return {@link NexRadDataModel}
		 */
		public NexRadDataModel build() {
			return (new NexRadDataModel(this));
		}
	}
}