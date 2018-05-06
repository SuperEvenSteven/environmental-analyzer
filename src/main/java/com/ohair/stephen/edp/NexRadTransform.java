package com.ohair.stephen.edp;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableRow;

@SuppressWarnings("serial")
public final class NexRadTransform extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {

	@Override
	public PCollection<TableRow> expand(PCollection<TableRow> input) {
		// TODO Auto-generated method stub
		return null;
	}
}
