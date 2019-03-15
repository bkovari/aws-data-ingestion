package com.amazonaws.batchdataingestion;

import java.util.List;

public interface IEMRClusterConfig {
	/**
	 * @param releaseLabel EMR cluster release label
	 * @param applications applications installed on cluster
	 */
	public void setApplications(String releaseLabel, List<String> applications);

	/**
	 * @param s3Path path to s3 script that describes a bootstrap configuration
	 */
	public void setBootstrapConfig(String s3Path);

	/**
	 * @param instanceType instance type for master node
	 */
	public void setMasterNode(String instanceType);

	/**
	 * @param instanceType  instance type for slave nodes
	 * @param instanceCount instance count for slave nodes
	 */
	public void setCoreNode(String instanceType, Integer instanceCount);
}
