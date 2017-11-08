package com.ibm.crail.storage.blkdev;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.metadata.DataNodeInfo;
import com.ibm.crail.storage.StorageClient;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.crail.storage.blkdev.client.BlkDevStorageEndpoint;

import java.io.IOException;
import org.slf4j.Logger;

public class BlkDevStorageClient implements StorageClient{
    private static final Logger LOG = CrailUtils.getLogger();

	public void printConf(Logger logger) {
		BlkDevStorageConstants.printConf(logger);
	}

	public void init(CrailConfiguration crailConfiguration, String[] args) throws IOException {
		BlkDevStorageConstants.updateConstants(crailConfiguration);
		BlkDevStorageConstants.verify();
	}

	public StorageEndpoint createEndpoint(DataNodeInfo info) throws IOException {
		return new BlkDevStorageEndpoint();
	}

	public void close() throws Exception {
	}

}
