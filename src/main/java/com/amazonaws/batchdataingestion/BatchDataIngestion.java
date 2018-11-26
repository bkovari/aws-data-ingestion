package com.amazonaws.batchdataingestion;

import java.util.Arrays;

public class BatchDataIngestion {

	private EMRCluster getConfiguredCluster() {

		EMRCluster cluster = new EMRCluster("MyAWSCluster", "eu-central-1");
		cluster.setApplications("emr-5.17.0", Arrays.asList("Sqoop", "Hadoop", "Hive"));
		cluster.setBootstrapConfig("s3://batchdataingestion/scripts/bootstrap.sh");
		cluster.setMasterNode("m4.xlarge");
		cluster.setCoreNode("m4.large", 2);
		return cluster;
	}

	public static void main(String[] args) {
		BatchDataIngestion dataIngestionApplication = new BatchDataIngestion();
		EMRCluster cluster = dataIngestionApplication.getConfiguredCluster();

		if (!EMRCluster.isClusterRunning(cluster.getName(), cluster.getRegion())) {
			cluster.start(true);
			cluster.addJob("OracleIngestion", "s3://batchdataingestion/scripts/hive_oracle_ingestion.sh");
		}
	}
}
