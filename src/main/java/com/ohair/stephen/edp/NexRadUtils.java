package com.ohair.stephen.edp;

import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;

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

/**
 * Originally sourced from Valliappa Lakshmanan's GitHub project: <a href=
 * "https://github.com/GoogleCloudPlatform/training-data-analyst/blob/master/blogs/nexrad2/src/com/google/cloud/public_datasets/nexrad2/APPipeline.java"
 * >original source reference</a>
 */
public final class NexRadUtils {

	public static List<String> getTarNameParams(Options options) {
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
}