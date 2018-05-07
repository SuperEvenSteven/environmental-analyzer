package com.ohair.stephen.edp.model;

import static org.junit.Assert.assertNotNull;

import java.util.Date;

import org.junit.Test;

import com.ohair.stephen.edp.model.GSODDataModel.ModelBuilder;


public class TestGSODDataModel {

	@Test
	public void testCanBuildDataModel() {
		// create {@link CombinedDataModel} table row
		ModelBuilder builder = new GSODDataModel.ModelBuilder();
		builder.setStationName("stationName");
		builder.setDateUtc(new Date());
		builder.setMeanDegreesC(20.0D);
		builder.setMinDegreesC(20.0D);
		builder.setMaxDegreesC(20.0D);
		builder.setTempReadCounts(1);
		builder.setPrecipitationCm(0.354);
		GSODDataModel model = builder.build();
		assertNotNull(model);
	}

}
