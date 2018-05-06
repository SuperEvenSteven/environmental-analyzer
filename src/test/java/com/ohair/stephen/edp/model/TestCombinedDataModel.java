package com.ohair.stephen.edp.model;

import static org.junit.Assert.assertNotNull;

import java.util.Date;

import org.junit.Test;

import com.ohair.stephen.edp.model.CombinedDataModel.ModelBuilder;

public class TestCombinedDataModel {

	@Test
	public void testCanBuildDataModel() {
		// create {@link CombinedDataModel} table row
		ModelBuilder builder = new CombinedDataModel.ModelBuilder();
		builder.setStationName("stationName");
		builder.setDateUtc(new Date());
		builder.setMeanDegreesC(20.0D);
		builder.setMinDegreesC(20.0D);
		builder.setMaxDegreesC(20.0D);
		builder.setTempReadCounts(1);
		builder.setPrecipitationCm(0.354);
		CombinedDataModel model = builder.build();
		assertNotNull(model);
	}

}
