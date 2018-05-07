package com.ohair.stephen.edp.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

/**
 * Represents the combined model of GSOD and NexRAD data.
 * 
 * @author Stephen O'Hair
 *
 */
@ThreadSafe
public final class CombinedDataModel {

    /*
     * Private member variables
     */
    private final String stationName;
    private final Date dateUtc;
    private final double meanDegreesC;
    private final double minDegreesC;
    private final double maxDegreesC;
    private final int tempReadCounts;
    private final double precipitationCm;

    /**
     * Constructor.
     *
     * @param builder - object builder
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
     * Returns this as a BigQuery TableSchema should it need to be persisted
     * there.
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
     * Builder pattern as per: https://stackoverflow.com/a/20940894/370003
     */
    public static class ModelBuilder {
        /*
         * Private member variables
         */
        private String stationName;
        private Date dateUtc;
        private double meanDegreesC;
        private double minDegreesC;
        private double maxDegreesC;
        private int tempReadCounts;
        private double precipitationCm;

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

        public ModelBuilder setMeanDegreesC(double meanDegreesC) {
            this.meanDegreesC = meanDegreesC;
            return this;
        }

        public ModelBuilder setMinDegreesC(double minDegreesC) {
            this.minDegreesC = minDegreesC;
            return this;
        }

        public ModelBuilder setMaxDegreesC(double maxDegreesC) {
            this.maxDegreesC = maxDegreesC;
            return this;
        }

        public ModelBuilder setTempReadCounts(int tempReadCounts) {
            this.tempReadCounts = tempReadCounts;
            return this;
        }

        public ModelBuilder setPrecipitationCm(double precipitationCm) {
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
