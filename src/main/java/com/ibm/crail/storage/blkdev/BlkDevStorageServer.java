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


import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.storage.StorageResource;
import org.apache.crail.storage.StorageServer;
import org.apache.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;

public class BlkDevStorageServer implements StorageServer {
	private static final Logger LOG = CrailUtils.getLogger();

	private InetSocketAddress storageAddr;
	private Path path;
	private boolean isAlive;
	private boolean initialized;
	private long addr;
	private long alignedSize;

	public BlkDevStorageServer() {
		this.isAlive = false;
		this.initialized = false;
	}

	public void init(CrailConfiguration crailConfiguration, String[] args) throws Exception {
		if (initialized) {
			throw new IOException("BlkDevStorageTier already initialized");
		}
		initialized = true;

		BlkDevStorageConstants.init(crailConfiguration, args);
		BlkDevStorageConstants.verify();

		String ipAddr = BlkDevStorageConstants.STORAGE_BLKDEV_IP;
		int port = BlkDevStorageConstants.STORAGE_BLKDEV_PORT;
		storageAddr = new InetSocketAddress(ipAddr, port);

		isAlive = true;
		alignedSize = BlkDevStorageConstants.STORAGE_SIZE -
				(BlkDevStorageConstants.STORAGE_SIZE % BlkDevStorageConstants.ALLOCATION_SIZE);
		addr = 0;

	}

	public void printConf(Logger log) {
		BlkDevStorageConstants.printTargetConf(log);
	}

	public StorageResource allocateResource() {
		StorageResource resource = null;
		if (alignedSize > 0) {
			LOG.info("new block, length " + BlkDevStorageConstants.ALLOCATION_SIZE);
			alignedSize -= BlkDevStorageConstants.ALLOCATION_SIZE;
			resource = StorageResource.createResource(addr, (int) BlkDevStorageConstants.ALLOCATION_SIZE, 0);
			addr += BlkDevStorageConstants.ALLOCATION_SIZE;
		}
		return resource;
	}

	public void run() {
		LOG.info("BlkDevStorageTier started");
	}

	public boolean isAlive() {
		return isAlive;
	}

	public InetSocketAddress getAddress() {
		return storageAddr;
	}
}
