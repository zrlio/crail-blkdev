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
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.concurrent.*;

import com.ibm.crail.CrailBuffer;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.memory.BufferCache;
import com.ibm.crail.metadata.BlockInfo;
import com.ibm.crail.storage.StorageFuture;
import com.ibm.crail.storage.blkdev.BlkDevBufferCache;
import com.ibm.crail.storage.blkdev.BlkDevStorageConstants;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.jaio.*;
import com.ibm.crail.utils.CrailUtils;
import org.slf4j.Logger;

public class BlkDevStorageEndpoint implements StorageEndpoint {
	private static final Logger LOG = CrailUtils.getLogger();

	private final File file;
	private final Semaphore concurrentOps;
	private final AsynchronousIOQueue queue;
	private final BlockingQueue<AsynchronousIOResultArray<BlkDevStorageFuture>> results;
	private final ThreadLocal<AsynchronousIOOperationArray> readOp;
	private final ThreadLocal<AsynchronousIOOperationArray> writeOp;
	private final BufferCache cache;

	public BlkDevStorageEndpoint(String devName) throws IOException {
		if (BlkDevStorageUtils.fileBlockOffset(CrailConstants.DIRECTORY_RECORD) != 0) {
			throw new IllegalArgumentException("Block device requires directory record size to be block aligned");
		}
		Path path = FileSystems.getDefault().getPath(devName);
		this.file = new File(path, OpenOption.READ, OpenOption.WRITE, OpenOption.DIRECT, OpenOption.SYNC);
		this.concurrentOps = new Semaphore(BlkDevStorageConstants.QUEUE_DEPTH, true);
		this.queue = new AsynchronousIOQueue(BlkDevStorageConstants.QUEUE_DEPTH);
		this.results = new ArrayBlockingQueue(BlkDevStorageConstants.QUEUE_DEPTH);
		int i = results.remainingCapacity();
		while (i-- > 0) {
			try {
				results.put(new AsynchronousIOResultArray(BlkDevStorageConstants.QUEUE_DEPTH));
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
		this.readOp = new ThreadLocal() {
			@Override
			protected AsynchronousIOOperationArray initialValue() {
				AsynchronousIOOperationArray ops = new AsynchronousIOOperationArray(1);
				ops.set(0, new AsynchronousIORead());
				return ops;
			}
		};
		this.writeOp = new ThreadLocal() {
			@Override
			protected AsynchronousIOOperationArray initialValue() {
				AsynchronousIOOperationArray ops = new AsynchronousIOOperationArray(1);
				ops.set(0, new AsynchronousIOWrite());
				return ops;
			}
		};
		this.cache = new BlkDevBufferCache();
	}

	enum Operation {
		WRITE,
		READ;

		private Operation() {}
	}

	StorageFuture Op(Operation op, CrailBuffer buffer, BlockInfo blockInfo, long remoteOffset) throws IOException, InterruptedException {
		if (buffer.remaining() > CrailConstants.BLOCK_SIZE){
			throw new IOException("write size too large " + buffer.remaining());
		}
		if (buffer.remaining() <= 0){
			throw new IOException("write size too small, len " + buffer.remaining());
		}
		if (buffer.position() < 0){
			throw new IOException("local offset too small " + buffer.position());
		}
		if (remoteOffset < 0){
			throw new IOException("remote offset too small " + remoteOffset);
		}

		if (blockInfo.getAddr() + remoteOffset + buffer.remaining() > BlkDevStorageConstants.STORAGE_LIMIT){
			long tmpAddr = blockInfo.getAddr() + remoteOffset + buffer.remaining();
			throw new IOException("remote fileOffset + remoteOffset + len too large " + tmpAddr);
		}

//		LOG.debug("op = " + op.name() +
//				", position = " + buffer.position() +
//				", localOffset = " + buffer.position() +
//				", remoteOffset = " + remoteOffset +
//				", remoteAddr = " + blockInfo.getAddr() +
//				", len = " + buffer.remaining());

		while (!concurrentOps.tryAcquire()) {
			// We need to be careful here as this uses ops thread local data, i.e.
			// this should _not_ be moved after readOp/writeOp access
			poll();
		}

		boolean aligned = BlkDevStorageUtils.fileBlockOffset(remoteOffset) == 0
				&& BlkDevStorageUtils.fileBlockOffset(buffer.remaining()) == 0
				&& BlkDevStorageUtils.fileBlockOffset(buffer.address() + buffer.position()) == 0;
		long fileOffset = BlkDevStorageUtils.fileOffset(blockInfo, remoteOffset);
		AsynchronousIOOperationArray ops = null;
		BlkDevStorageFuture future = null;
		if (aligned) {
//			LOG.debug("aligned");
			future = new BlkDevStorageFuture(this, buffer.remaining());
			switch(op) {
				case READ:
					ops = readOp.get();
					AsynchronousIORead<BlkDevStorageFuture> read =
							(AsynchronousIORead<BlkDevStorageFuture>) ops.get(0);
					read.set(file, buffer.getByteBuffer(), fileOffset, future);
					break;
				case WRITE:
					ops = writeOp.get();
					AsynchronousIOWrite<BlkDevStorageFuture> write =
							(AsynchronousIOWrite<BlkDevStorageFuture>) ops.get(0);
					write.set(file, buffer.getByteBuffer(), fileOffset, future);
					break;
			}
		} else {
			long alignedSize = BlkDevStorageUtils.alignLength(remoteOffset, buffer.remaining());
			long alignedFileOffset = BlkDevStorageUtils.alignOffset(fileOffset);

			CrailBuffer stagingBuffer = cache.getBuffer();
			stagingBuffer.clear();
			stagingBuffer.limit((int)alignedSize);
			//TODO: make sure buffer is aligned! We can align ourselfs or try to enforce java memalign option.
			try {
				switch(op) {
					case READ: {
						future = new BlkDevStorageUnalignedReadFuture(this, buffer, blockInfo, remoteOffset, stagingBuffer);
						ops = readOp.get();
						AsynchronousIORead<BlkDevStorageFuture> read = (AsynchronousIORead<BlkDevStorageFuture>) ops.get(0);
						read.set(file, stagingBuffer.getByteBuffer(), alignedFileOffset, future);
						break;
					}
					case WRITE: {
						if (alignedFileOffset != fileOffset) {
							// Append only file system and dir entries (512B) are always aligned!
							// XXX this only works with write append (currently only supported interface)!
							stagingBuffer.limit((int) Files.blockSize());
							future = new BlkDevStorageUnalignedRMWFuture(this, buffer, blockInfo, remoteOffset, stagingBuffer);
							ops = readOp.get();
							AsynchronousIORead<BlkDevStorageFuture> read = (AsynchronousIORead<BlkDevStorageFuture>) ops.get(0);
							read.set(file, stagingBuffer.getByteBuffer(), alignedFileOffset, future);
						} else {
							// If the file offset is aligned we do not need to read
							future = new BlkDevStorageUnalignedWriteFuture(this, buffer, blockInfo, remoteOffset, stagingBuffer);
							ops = writeOp.get();
							AsynchronousIOWrite<BlkDevStorageFuture> write =
									(AsynchronousIOWrite<BlkDevStorageFuture>) ops.get(0);
							write.set(file, stagingBuffer.getByteBuffer(), fileOffset, future);
						}
						break;
					}
				}

			} catch (NoSuchFieldException e) {
				throw new IOException(e);
			} catch (IllegalAccessException e) {
				throw new IOException(e);
			}
		}
		queue.submit(ops);

		return future;
	}

	void poll() throws IOException, InterruptedException {
		AsynchronousIOResultArray<BlkDevStorageFuture> result = results.take();
		int n = queue.poll(result);
		concurrentOps.release(n);
		for (int i = 0; i < n; i++) {
			BlkDevStorageFuture future = result.get(i).getAttachment();
			future.signal(result.get(i).getResult());
		}
		results.put(result);
	}

	void putBuffer(CrailBuffer buffer) throws IOException {
		cache.putBuffer(buffer);
	}

	public StorageFuture write(CrailBuffer buffer, BlockInfo blockInfo, long remoteOffset) throws IOException, InterruptedException {
		return Op(Operation.WRITE, buffer, blockInfo, remoteOffset);
	}

	public StorageFuture read(CrailBuffer buffer, BlockInfo blockInfo, long remoteOffset) throws IOException, InterruptedException {
		return Op(Operation.READ, buffer, blockInfo, remoteOffset);
	}

	public void close() throws IOException, InterruptedException {
		file.close();
		queue.close();
	}

	public boolean isLocal() {
		return false;
	}

}
