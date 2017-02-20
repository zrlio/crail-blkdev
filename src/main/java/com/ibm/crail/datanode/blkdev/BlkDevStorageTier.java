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

package com.ibm.crail.datanode.blkdev;

import com.google.common.primitives.Ints;
import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.datanode.blkdev.client.BlkDevStorageEndpoint;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.storage.StorageTier;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.jaio.Files;
import com.ibm.crail.namenode.protocol.DataNodeStatistics;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.FileSystems;
import java.nio.file.Path;

public class BlkDevStorageTier extends StorageTier {

	private static final Logger LOG = CrailUtils.getLogger();
	private InetSocketAddress datanodeAddr;

	public InetSocketAddress getAddress() {
		return datanodeAddr;
	}

	public void printConf(Logger logger) {
		BlkDevStorageConstants.printConf(logger);
	}

	public void init(CrailConfiguration crailConfiguration, String[] args) throws IOException {
		BlkDevStorageConstants.updateConstants(crailConfiguration);
		BlkDevStorageConstants.verify();
	}

	public StorageEndpoint createEndpoint(InetSocketAddress inetSocketAddress) throws IOException {
		return new BlkDevStorageEndpoint();
	}

	public void run() throws Exception {
		LOG.info("initalizing block device datanode");

		InetAddress address = InetAddress.getByAddress(Ints.toByteArray((int)(System.nanoTime() & 0xFFFFFFFF)));
		int port = 12345;
		datanodeAddr = new InetSocketAddress(address, port);

		String directory = BlkDevStorageConstants.DATA_PATH;
		LOG.info("Block device " + directory);

		Path path = FileSystems.getDefault().getPath(directory);
		long size = Files.size(path);
		LOG.info("Block device size " + size);

//		if (CrailConstants.STORAGE_LIMIT > size) {
//			throw new Exception("Asked for " + CrailConstants.STORAGE_LIMIT + " but only " + size +
//					" bytes are" + "available on block device");
//		}

		long alignedSize = BlkDevStorageConstants.STORAGE_LIMIT - (BlkDevStorageConstants.STORAGE_LIMIT % BlkDevStorageConstants.ALLOCATION_SIZE);
		long addr = 0;
		while (alignedSize > 0) {
			DataNodeStatistics statistics = this.getDataNode();
			LOG.info("datanode statistics, freeBlocks " + statistics.getFreeBlockCount());

			LOG.info("new block, length " + BlkDevStorageConstants.ALLOCATION_SIZE);
			LOG.debug("block stag 0, addr 0, length " + BlkDevStorageConstants.ALLOCATION_SIZE);
			alignedSize -= BlkDevStorageConstants.ALLOCATION_SIZE;
			this.setBlock(addr, (int) BlkDevStorageConstants.ALLOCATION_SIZE, 0);
			addr += BlkDevStorageConstants.ALLOCATION_SIZE;
		}

		while (true) {
			DataNodeStatistics statistics = this.getDataNode();
			LOG.info("datanode statistics, freeBlocks " + statistics.getFreeBlockCount());
			Thread.sleep(2000);
		}
	}

	public void close() throws Exception {

	}
}
