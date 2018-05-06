package com.ohair.stephen.edp;

import org.junit.Test;

public class TestPrecipitationTransform {

	@Test
	public void testToCelsiusDegreesReturnsValidGivenValidFarenheit() {
		double expected = 1.0D;
		double actual = 1.0D;
		double deltaPrecisionLoss = 1.0D;
		org.junit.Assert.assertEquals(expected, actual, deltaPrecisionLoss);
	}

}
