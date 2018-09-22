package com.amazonaws.dataingestion;

import java.util.List;

public interface IEMRClusterConfig {
	public void setApplications(String releaseLabel, List<String> applications);
	public void setBootstrapConfig(String s3Path);
	public void setMasterNode(String instanceType);
	public void setCoreNode(String instanceType, Integer instanceCount);

}
