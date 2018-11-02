package com.amazonaws.batchdataingestion;

import java.util.List;

/**
 * @author Bence
 *
 */
public interface IEMRClusterConfig {
	/**
	 * @param releaseLabel
	 * @param applications
	 */
	public void setApplications(String releaseLabel, List<String> applications);

	/**
	 * @param s3Path
	 */
	public void setBootstrapConfig(String s3Path);

	/**
	 * @param instanceType
	 */
	public void setMasterNode(String instanceType);

	/**
	 * @param instanceType
	 * @param instanceCount
	 */
	public void setCoreNode(String instanceType, Integer instanceCount);

}
