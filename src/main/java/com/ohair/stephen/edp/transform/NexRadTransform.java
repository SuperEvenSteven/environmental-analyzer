package com.ohair.stephen.edp.transform;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.public_datasets.nexrad2.APDetector;
import com.google.cloud.public_datasets.nexrad2.APDetector.AnomalousPropagation;
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
        public void processElement(ProcessContext c) throws IOException, ParseException {

            String tarFile = c.element();
            ModelBuilder builder = new NexRadDataModel.ModelBuilder();
            try (GcsNexradL2Read hourly = new GcsNexradL2Read(tarFile)) {
                // this volume may contain data for multiple weather stations
                for (RadialDatasetSweep volume : hourly.getVolumeScans()) {
                    builder.setDateUtc(formatDateUtc(""));
                    List<AnomalousPropagation> apPixels = APDetector.findAP(volume);
                    log.info("Found " + apPixels.size() + " AP pixels");
                    for (AnomalousPropagation ap : apPixels) {
                        // 
                       // c.output(ap);
                    }
                }
            } catch (Exception e) {
                log.error("Skipping " + tarFile, e);
            }

            builder.setDateUtc(formatDateUtc(""));
            c.output(builder.build());
        }

        /**
         * Takes a NexRad date time and formats it to a {@link java.util.Data}
         * as UTC.
         * 
         * @param date
         * @throws ParseException
         */
        private static Date formatDateUtc(String date) throws ParseException {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            df.setTimeZone(TimeZone.getTimeZone("UTC"));
            return df.parse(date);
        }
    }
}
