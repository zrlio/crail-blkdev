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

package com.ibm.crail.datanode.blkdev.client;

import com.ibm.crail.namenode.protocol.BlockInfo;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BlkDevDataUnalignedWriteFuture extends BlkDevDataUnalignedFuture {

	public BlkDevDataUnalignedWriteFuture(BlkDevDataNodeEndpoint endpoint, ByteBuffer buffer, BlockInfo remoteMr, long remoteOffset,
	                                      ByteBuffer stagingBuffer) throws NoSuchFieldException, IllegalAccessException {
		super(endpoint, buffer,remoteMr, remoteOffset, stagingBuffer);
		long srcAddr = BlkDevDataNodeUtils.getAddress(buffer) + localOffset;
		long dstAddr = BlkDevDataNodeUtils.getAddress(stagingBuffer) + BlkDevDataNodeUtils.fileBlockOffset(remoteOffset);
		unsafe.copyMemory(srcAddr, dstAddr, len);
	}

	@Override
	public void signal(int result) throws IOException, InterruptedException {
		endpoint.putBuffer(stagingBuffer);
		super.signal(result);
	}
}
