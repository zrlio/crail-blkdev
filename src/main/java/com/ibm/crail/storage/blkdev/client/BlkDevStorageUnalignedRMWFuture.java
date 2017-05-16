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

package com.ibm.crail.storage.blkdev.client;


import com.ibm.crail.CrailBuffer;
import com.ibm.crail.metadata.BlockInfo;
import com.ibm.crail.storage.StorageFuture;
import com.ibm.crail.storage.StorageResult;
import com.ibm.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class BlkDevStorageUnalignedRMWFuture extends BlkDevStorageUnalignedFuture {

	private static final Logger LOG = CrailUtils.getLogger();

	private StorageFuture future;

	private volatile boolean done;

	public BlkDevStorageUnalignedRMWFuture(BlkDevStorageEndpoint endpoint, CrailBuffer buffer, BlockInfo remoteMr, long remoteOffset,
										   CrailBuffer stagingBuffer) throws NoSuchFieldException, IllegalAccessException {
		super(endpoint, buffer, remoteMr, remoteOffset, stagingBuffer);
		future = null;
		done = false;
	}

	@Override
	public boolean isDone() {
		return done;
	}

	@Override
	public void signal(int result) throws IOException, InterruptedException {
		if (result >= 0) {
			long srcAddr = buffer.address() + localOffset;
			long dstAddr = stagingBuffer.address() + BlkDevStorageUtils.fileBlockOffset(remoteOffset);
			unsafe.copyMemory(srcAddr, dstAddr, len);

			stagingBuffer.clear();
			int alignedLen = (int) BlkDevStorageUtils.alignLength(remoteOffset, len);
			stagingBuffer.limit(alignedLen);
			future = endpoint.write(stagingBuffer, blockInfo, BlkDevStorageUtils.alignOffset(remoteOffset));
		}
		super.signal(result);
	}

	@Override
	public StorageResult get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		if (!done) {
			super.get(timeout, unit);
			future.get(timeout, unit);
			try {
				endpoint.putBuffer(stagingBuffer);
			} catch (IOException e) {
				throw new ExecutionException(e);
			}
			done = true;
		}
		return this;
	}
}
