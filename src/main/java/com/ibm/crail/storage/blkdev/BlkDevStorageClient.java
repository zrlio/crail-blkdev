package com.ibm.crail.storage.blkdev;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.metadata.DataNodeInfo;
import com.ibm.crail.storage.StorageClient;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.crail.storage.blkdev.client.BlkDevStorageEndpoint;

import java.io.IOException;
import java.util.HashMap;
import org.slf4j.Logger;

public class BlkDevStorageClient implements StorageClient{
	private HashMap<Long, String> nodeMap;

	public void printConf(Logger logger) {
		BlkDevStorageConstants.printClientConf(logger);
	}

	public void init(CrailConfiguration crailConfiguration, String[] args) throws IOException {
		nodeMap  = new HashMap<Long, String>();
		BlkDevStorageConstants.updateClientConstants(nodeMap, crailConfiguration);
		BlkDevStorageConstants.verify();
	}

	public StorageEndpoint createEndpoint(DataNodeInfo info) throws IOException {
		long key = BlkDevStorageConstants.calcKey(info.getIpAddress(), info.getPort());
		String vDevPath = nodeMap.get(key);
		return new BlkDevStorageEndpoint(vDevPath);
	}

	public void close() throws Exception {
	}

}
