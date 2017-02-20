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

import com.ibm.crail.storage.DataResult;
import com.ibm.crail.utils.CrailUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class BlkDevStorageFuture implements Future<DataResult>, DataResult {
	protected final BlkDevStorageEndpoint endpoint;

	private static final Logger LOG = CrailUtils.getLogger();

	private volatile boolean done;
	private volatile int result;
	private int len;

	private final int hashCode;

	static final AtomicInteger hash = new AtomicInteger(0);

	public BlkDevStorageFuture(BlkDevStorageEndpoint endpoint, int len) {
		this.endpoint = endpoint;
		this.done = false;
		this.result = 0;
		this.len = len;
		this.hashCode = hash.getAndIncrement();
	}

	void signal(int result) throws IOException, InterruptedException {
		if (done) {
			throw new IOException("already done");
		}
		this.result = result;
		done = true;
	}

	public int getLen() {
		return len;
	}

	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	public boolean isCancelled() {
		return false;
	}

	public boolean isDone() {
		return done;
	}

	public final DataResult get() throws InterruptedException, ExecutionException {
		try {
			return get(-1, null);
		} catch (TimeoutException e) {
			throw new InterruptedException(e.toString());
		}
	}

	public DataResult get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		if (!done) {
			// TODO: timeout
			do {
				try {
					endpoint.poll();
				} catch (IOException e) {
					throw new ExecutionException(e);
				}
			} while (!done);
			if (result < 0) {
				throw new ExecutionException("Error: " + result) {
				};
			} else if (result != len) {
				throw new ExecutionException("Only " + result + " bytes were read/written instead of " + len) {
				};
			}
		}
		return this;
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public boolean equals(Object o) {
		return hashCode == o.hashCode();
	}
}
