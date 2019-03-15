package com.amazonaws.batchdataingestion;

public class BatchDataIngestion {

	public static void main(String[] args) {

		EMRCluster cluster = EMRClusterFactory.getConfiguredCluster("MyAWSCluster", "eu-central-1");

		if (!EMRCluster.isClusterRunning(cluster.getName(), cluster.getRegion())) {
			cluster.start(true);
			cluster.addJob("OracleIngestion", "s3://batchdataingestion/scripts/hive_oracle_ingestion.sh");
		}
	}
}
