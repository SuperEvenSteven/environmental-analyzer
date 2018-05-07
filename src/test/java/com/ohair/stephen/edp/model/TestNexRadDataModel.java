package com.ohair.stephen.edp.model;

import static org.junit.Assert.assertNotNull;

import java.util.Date;

import org.junit.Test;

import com.ohair.stephen.edp.model.NexRadDataModel.ModelBuilder;

public class TestNexRadDataModel {

	@Test
	public void testCanBuildDataModel() {
		// create {@link CombinedDataModel} table row
		ModelBuilder builder = new NexRadDataModel.ModelBuilder();
		builder.setStationName("stationName");
		builder.setDateUtc(new Date());
		NexRadDataModel model = builder.build();
		assertNotNull(model);
	}

}
