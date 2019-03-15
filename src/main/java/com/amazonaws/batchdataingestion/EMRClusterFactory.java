package com.amazonaws.batchdataingestion;

import java.util.Arrays;

public class EMRClusterFactory {

	/**
	 * Factory for EMR clusters.
	 * 
	 * @param name   cluster name
	 * @param region cluster region
	 * @return configured cluster
	 */
	public static EMRCluster getConfiguredCluster(String name, String region) {

		EMRCluster cluster = new EMRCluster(name, region);
		cluster.setApplications("emr-5.17.0", Arrays.asList("Sqoop", "Hadoop", "Hive"));
		cluster.setBootstrapConfig("s3://batchdataingestion/scripts/bootstrap.sh");
		cluster.setMasterNode("m4.xlarge");
		cluster.setCoreNode("m4.large", 2);
		return cluster;
	}
}
