package com.ohair.stephen.edp;

import java.util.Date;

/**
 * Builder patterns for {@link CombinedDataModel}.
 * 
 * @author Stephen O'Hair
 *
 */
public class CombinedDataModelBuilder {

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
	 * @param stationName
	 * @param dateUtc
	 * @param meanDegreesC
	 * @param minDegreesC
	 * @param maxDegrees
	 * @param tempReadCounts
	 * @param precipitationCm
	 */
	private CombinedDataModelBuilder(String stationName, Date dateUtc, float meanDegreesC, float minDegreesC,
			float maxDegrees, int tempReadCounts, float precipitationCm) {
		this.stationName = stationName;
		this.dateUtc = dateUtc;
		this.meanDegreesC = meanDegreesC;
		this.minDegreesC = minDegreesC;
		this.maxDegreesC = maxDegrees;
		this.tempReadCounts = tempReadCounts;
		this.precipitationCm = precipitationCm;
	}

	/*
	 * Public mutator methods
	 */

	public CombinedDataModelBuilder setStationName(String stationName) {
		this.stationName = stationName;
		return this;
	}

	public CombinedDataModelBuilder setDateUtc(Date dateUtc) {
		this.dateUtc = dateUtc;
		return this;
	}

	public CombinedDataModelBuilder setMeanDegreesC(float meanDegreesC) {
		this.meanDegreesC = meanDegreesC;
		return this;
	}

	public CombinedDataModelBuilder setMinDegreesC(float minDegreesC) {
		this.minDegreesC = minDegreesC;
		return this;
	}

	public CombinedDataModelBuilder setMaxDegreesC(float maxDegreesC) {
		this.maxDegreesC = maxDegreesC;
		return this;
	}

	public CombinedDataModelBuilder setTempReadCounts(int tempReadCounts) {
		this.tempReadCounts = tempReadCounts;
		return this;
	}

	public CombinedDataModelBuilder setPrecipitationCm(int precipitationCm) {
		this.precipitationCm = precipitationCm;
		return this;
	}

	/**
	 * Builder build.
	 * 
	 * @return {@link CombinedDataModel}
	 */
	public CombinedDataModel build() {
		return new CombinedDataModel(stationName, dateUtc, meanDegreesC, minDegreesC, maxDegreesC, tempReadCounts,
				precipitationCm);
	}

}
