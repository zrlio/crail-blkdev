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

import java.io.IOException;
import org.apache.crail.CrailBuffer;
import org.apache.crail.metadata.BlockInfo;

public class BlkDevStorageUnalignedWriteFuture extends BlkDevStorageUnalignedFuture {

	public BlkDevStorageUnalignedWriteFuture(BlkDevStorageEndpoint endpoint, CrailBuffer buffer, BlockInfo blockInfo, long remoteOffset,
											 CrailBuffer stagingBuffer) throws NoSuchFieldException, IllegalAccessException {
		super(endpoint, buffer,blockInfo, remoteOffset, stagingBuffer);
		long srcAddr = buffer.address() + localOffset;
		long dstAddr = stagingBuffer.address() + BlkDevStorageUtils.fileBlockOffset(remoteOffset);
		unsafe.copyMemory(srcAddr, dstAddr, len);
	}

	@Override
	public void signal(int result) throws IOException, InterruptedException {
		endpoint.putBuffer(stagingBuffer);
		super.signal(result);
	}
}
