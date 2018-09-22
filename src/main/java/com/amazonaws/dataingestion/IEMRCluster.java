package com.amazonaws.dataingestion;

public interface IEMRCluster {
	public void start(boolean enableDebugging);
	public void stop();
	public void addClusterJob();
}
