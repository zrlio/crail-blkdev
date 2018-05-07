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
import java.lang.reflect.Field;
import org.apache.crail.CrailBuffer;
import org.apache.crail.metadata.BlockInfo;
import sun.misc.Unsafe;

public abstract class BlkDevStorageUnalignedFuture extends BlkDevStorageFuture {
	protected final CrailBuffer buffer;
	protected final long localOffset;
	protected final BlockInfo blockInfo;
	protected final long remoteOffset;
	protected final int len;
	protected final CrailBuffer stagingBuffer;
	protected Unsafe unsafe;

	public BlkDevStorageUnalignedFuture(BlkDevStorageEndpoint endpoint, CrailBuffer buffer, BlockInfo blockInfo,
										long remoteOffset, CrailBuffer stagingBuffer) throws NoSuchFieldException, IllegalAccessException {
		super(endpoint, buffer.remaining());
		this.buffer = buffer;
		this.localOffset = buffer.position();
		this.blockInfo = blockInfo;
		this.remoteOffset = remoteOffset;
		this.len = buffer.remaining();
		this.stagingBuffer = stagingBuffer;
		this.unsafe = getUnsafe();
	}

	@Override
	public void signal(int result) throws IOException, InterruptedException {
		if (result <= 0) {
			super.signal(result);
		} else {
			super.signal(len);
		}
	}

	private Unsafe getUnsafe() throws NoSuchFieldException, IllegalAccessException {
		Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
		theUnsafe.setAccessible(true);
		Unsafe unsafe = (Unsafe) theUnsafe.get(null);
		return unsafe;
	}
}
