package com.amazonaws.batchdataingestion;

public interface IEMRCluster {
	/**
	 * @param enableDebugging enable debugging for EMR cluster
	 */
	public void start(boolean enableDebugging);

	/**
	 * Shutdown EMR cluster.
	 */
	public void stop();

	/**
	 * @param jobName bash script added as job to be run
	 * @param path    S3 path to the job script
	 */
	public void addJob(String jobName, String path);

}
