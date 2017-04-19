/*
 * Crail: A Multi-tiered Distributed Direct Access File System
 *
 * Author:
 * Jonas Pfefferle <jpf@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.crail.storage.blkdev;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.conf.CrailConstants;
import org.slf4j.Logger;

import java.io.IOException;

public class BlkDevStorageConstants {

	private final static String PREFIX = "crail.storage.blkdev";

	public static final String STORAGE_LIMIT_KEY = "storagelimit";
	public static long STORAGE_LIMIT = 1073741824; /* 1GB */

	public static final String ALLOCATION_SIZE_KEY = "allocationsize";
	public static long ALLOCATION_SIZE = 1073741824; /* 1GB */

	public static final String DATA_PATH_KEY = "datapath";
	public static String DATA_PATH;

	public static final String QUEUE_DEPTH_KEY = "queuedepth";
	public static int QUEUE_DEPTH = 16;

	private static String fullKey(String key) {
		return PREFIX + "." + key;
	}

	private static String get(CrailConfiguration conf, String key) {
		return conf.get(fullKey(key));
	}

	public static void updateConstants(CrailConfiguration conf) {
		String arg = get(conf, STORAGE_LIMIT_KEY);
		if (arg != null) {
			STORAGE_LIMIT = Long.parseLong(arg);
		}

		arg = get(conf, ALLOCATION_SIZE_KEY);
		if (arg != null) {
			ALLOCATION_SIZE = Long.parseLong(arg);
		}

		arg = get(conf, DATA_PATH_KEY);
		if (arg != null) {
			DATA_PATH = arg;
		}

		arg = get(conf, QUEUE_DEPTH_KEY);
		if (arg != null) {
			QUEUE_DEPTH = Integer.parseInt(arg);
		}
	}

	public static void verify() throws IOException {
		if (ALLOCATION_SIZE % CrailConstants.BLOCK_SIZE != 0){
			throw new IOException("allocationsize must be multiple of crail.blocksize");
		}
		if (STORAGE_LIMIT % ALLOCATION_SIZE != 0){
			throw new IOException("storageLimit must be multiple of allocationSize");
		}
	}

	public static void printConf(Logger logger) {
		logger.info(fullKey(STORAGE_LIMIT_KEY) + " " + STORAGE_LIMIT);
		logger.info(fullKey(ALLOCATION_SIZE_KEY) + " " + ALLOCATION_SIZE);
		logger.info(fullKey(DATA_PATH_KEY) + " " + DATA_PATH);
		logger.info(fullKey(QUEUE_DEPTH_KEY) + " " + QUEUE_DEPTH);
	}
}
