/*
 * Copyright (C) 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.public_datasets.nexrad2;

import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.ohair.stephen.edp.BatchProcessOptions;

/**
 * Originally sourced from Valliappa Lakshmanan's GitHub project: <a href=
 * "https://github.com/GoogleCloudPlatform/training-data-analyst/blob/master/blogs/nexrad2/src/com/google/cloud/public_datasets/nexrad2/APPipeline.java"
 * >original source reference</a>
 */
public final class NexRadUtils {

    public static List<String> getTarNameParams(BatchProcessOptions options) {
        // parse command-line options
        String[] radars = options.getRadars().split(",");
        int[] years = toIntArray(options.getYears().split(","));
        int[] months = toIntArray(options.getMonths().split(","));
        if (months.length == 0) {
            // all months
            months = new int[12];
            for (int i = 1; i <= 12; ++i) {
                months[i] = i;
            }
        }
        int[] days = toIntArray(options.getDays().split(","));

        // generate parameter options
        List<String> params = new ArrayList<>();
        for (String radar : radars) {
            for (int year : years) {
                for (int month : months) {
                    YearMonth yearMonthObject = YearMonth.of(year, month);
                    int maxday = yearMonthObject.lengthOfMonth();
                    if (days.length == 0) {
                        for (int day = 1; day <= maxday; ++day) {
                            params.add(radar + "," + year + "," + month + "," + day);
                        }
                    } else {
                        for (int day : days) {
                            if (day >= 1 && day <= maxday) {
                                params.add(radar + "," + year + "," + month + "," + day);
                            }
                        }
                    }
                }
            }
        }
        return params;
    }

    private static int[] toIntArray(String[] s) {
        int[] result = new int[s.length];
        for (int i = 0; i < result.length; ++i) {
            result[i] = Integer.parseInt(s[i]);
        }
        return result;
    }

    /**
     * Rebundling improves parallelism. Each worker in Apache Beam works on only
     * one bundle, so if the number of bundles less than the number of potential
     * workers, you will have limited parallelism. If that's case, use this
     * rebundle utility
     * 
     * @param name
     *            of rebundling transforms
     * @param inputs
     *            the collection to rebundle
     * @param nbundles
     *            number of bundles
     * @return a rebundled collection
     */
    @SuppressWarnings("serial")
    public static <T> PCollection<T> rebundle(String name, PCollection<T> inputs, int nbundles) {
        return inputs//
                .apply(name + "-1", ParDo.of(new DoFn<T, KV<Integer, T>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        T input = c.element();
                        Integer key = (int) (Math.random() * nbundles);
                        c.output(KV.of(key, input));
                    }
                })) //
                .apply(name + "-2", GroupByKey.<Integer, T> create())
                .apply(name + "-3", ParDo.of(new DoFn<KV<Integer, Iterable<T>>, T>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        for (T item : c.element().getValue()) {
                            c.output(item);
                        }
                    }
                }));
    }
}