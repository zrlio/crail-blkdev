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
import com.ibm.jaio.Files;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

public class BlkDevStorageUtils {

	public static long fileOffset(BlockInfo remoteMr, long remoteOffset) {
		return remoteMr.getAddr() + remoteOffset;
	}

	public static long fileBlockOffset(long fileOffset) {
		return fileOffset % Files.blockSize();
	}

	public static long alignLength(long remoteOffset, long len) {
		long alignedSize = len + fileBlockOffset(remoteOffset);
		if (fileBlockOffset(alignedSize) != 0) {
			alignedSize += Files.blockSize() - fileBlockOffset(alignedSize);
		}
		return alignedSize;
	}

	public static long alignOffset(long fileOffset) {
		return fileOffset - fileBlockOffset(fileOffset);
	}

	public static long getAddress(ByteBuffer buffer) {
		return ((DirectBuffer)buffer).address();
	}
}
