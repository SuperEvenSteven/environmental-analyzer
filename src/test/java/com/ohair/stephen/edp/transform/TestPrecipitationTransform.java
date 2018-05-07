package com.ohair.stephen.edp.transform;

import org.junit.Test;

import com.ohair.stephen.edp.transform.GSODTransform;

public class TestPrecipitationTransform {

	@Test
	public void testAsCelsiusDegreesReturnsZeroDegreesGivenZeroFarenheit() {
		double actual = GSODTransform.asCelsiusDeg(32.0F);
		double expected = 0D;
		double deltaPrecisionLoss = 1.0D;
		org.junit.Assert.assertEquals(expected, actual, deltaPrecisionLoss);
	}

	@Test
	public void testAsCelsiusDegreesGivenValidPositiveFarenheit() {
		double actual = GSODTransform.asCelsiusDeg(64.0F);
		double expected = 17.77D;
		double deltaPrecisionLoss = 2.0D;
		org.junit.Assert.assertEquals(expected, actual, deltaPrecisionLoss);
	}

	@Test
	public void testAsCelsiusDegreesGivenValidNegativeFarenheit() {
		double actual = GSODTransform.asCelsiusDeg(-10.0F);
		double expected = -23.33D;
		double deltaPrecisionLoss = 2.0D;
		org.junit.Assert.assertEquals(expected, actual, deltaPrecisionLoss);
	}
}
