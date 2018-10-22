package com.amazonaws.dataingestion;

/**
 * @author Bence
 *
 */
public interface IEMRCluster {
	/**
	 * @param enableDebugging
	 */
	public void start(boolean enableDebugging);

	/**
	 * 
	 */
	public void stop();

	/**
	 * @param jobName
	 * @param path
	 */
	public void addJob(String jobName, String path);

}
