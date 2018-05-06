package com.ohair.stephen.edp.transform;

import java.io.IOException;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.public_datasets.nexrad2.GcsNexradL2Read;
import com.ohair.stephen.edp.model.NexRadDataModel;
import com.ohair.stephen.edp.model.NexRadDataModel.ModelBuilder;
import com.ohair.stephen.edp.transform.GSODTransform.SimplifyAndFilterPrecipitationFn;

import ucar.nc2.dt.RadialDatasetSweep;

/**
 * The transformation of NexRad tar files to a {@link NexRadDataModel}.
 * 
 * @author Stephen O'Hair
 *
 */
@SuppressWarnings("serial")
public final class NexRadTransform extends PTransform<PCollection<String>, PCollection<NexRadDataModel>> {

	@Override
	public PCollection<NexRadDataModel> expand(PCollection<String> elements) {
		// NexRAD tar file... => filtered precipitation row
		PCollection<NexRadDataModel> p = elements.apply(ParDo.of(new AddReflectivityFn()));
		return p;
	}

	static class AddReflectivityFn extends DoFn<String, NexRadDataModel> {

		private static final Logger log = LoggerFactory.getLogger(SimplifyAndFilterPrecipitationFn.class);

		@ProcessElement
		public void processElement(ProcessContext c) throws IOException {
			
			String tarFile = c.element();
            try (GcsNexradL2Read hourly = new GcsNexradL2Read(tarFile)) {
              for (RadialDatasetSweep volume : hourly.getVolumeScans()) {
					// List<AnomalousPropagation> apPixels = APDetector.findAP(volume);
					// log.info("Found " + apPixels.size() + " AP pixels");
					// for (AnomalousPropagation ap : apPixels) {
					// c.output(ap);
					// }
              }
            } catch (Exception e) {
              log.error("Skipping " + tarFile, e);
            }
			
			
			
			
			ModelBuilder builder = new NexRadDataModel.ModelBuilder();
			c.output(builder.build());
		}
	}
}
