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

import com.ibm.crail.metadata.BlockInfo;

import java.io.IOException;
import java.nio.ByteBuffer;


public final class BlkDevStorageUnalignedReadFuture extends BlkDevStorageUnalignedFuture {

	public BlkDevStorageUnalignedReadFuture(BlkDevStorageEndpoint endpoint, ByteBuffer buffer, BlockInfo remoteMr,
											long remoteOffset, ByteBuffer stagingBuffer) throws NoSuchFieldException, IllegalAccessException {
		super(endpoint, buffer, remoteMr, remoteOffset, stagingBuffer);
	}

	@Override
	public void signal(int result) throws IOException, InterruptedException {
		if (result >= 0) {
			long srcAddr = BlkDevStorageUtils.getAddress(stagingBuffer) + BlkDevStorageUtils.fileBlockOffset(remoteOffset);
			long dstAddr = BlkDevStorageUtils.getAddress(buffer) + localOffset;
			unsafe.copyMemory(srcAddr, dstAddr, len);
		}
		super.signal(result);
		endpoint.putBuffer(stagingBuffer);
	}
}
