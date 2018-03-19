package com.ibm.crail.storage.blkdev;

import com.ibm.crail.storage.blkdev.client.BlkDevStorageEndpoint;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.crail.metadata.DataNodeInfo;
import org.apache.crail.storage.StorageClient;
import org.apache.crail.storage.StorageEndpoint;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.HashMap;

public class BlkDevStorageClient implements StorageClient{
	private HashMap<Long, String> nodeMap;

	public void printConf(Logger logger) {
		BlkDevStorageConstants.printClientConf(logger);
	}

	public void init(CrailConfiguration crailConfiguration, String[] args) throws IOException {
		nodeMap  = new HashMap<Long, String>();
		BlkDevStorageConstants.updateClientConstants(nodeMap, crailConfiguration);
	}

	public StorageEndpoint createEndpoint(DataNodeInfo info) throws IOException {
		long key = BlkDevStorageConstants.calcKey(info.getIpAddress(), info.getPort());
		String vDevPath = nodeMap.get(key);
		if (vDevPath == null) {
			String message = "No path for datanode with ip = ";
			byte rawIp[] = info.getIpAddress();
			for (int i = 0; i < rawIp.length; i++) {
				message += rawIp[i] & 0xff;
				if (i != (rawIp.length - 1)) {
					message += ".";
				}
			}
			message += " and port = " + info.getPort();
			throw new IllegalArgumentException(message);
		}
		return new BlkDevStorageEndpoint(vDevPath);
	}

	public void close() throws Exception {
	}

}
