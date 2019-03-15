package com.amazonaws.batchdataingestion;

public class BatchDataIngestion {

	private static EMRCluster _cluster;

	public static void main(String[] args) {

		_cluster = EMRClusterFactory.getConfiguredCluster("MyAWSCluster", "eu-central-1");

		if (!EMRCluster.isClusterRunning(_cluster.getName(), _cluster.getRegion())) {
			_cluster.start(true);
			_cluster.addJob("OracleIngestion", "s3://batchdataingestion/scripts/hive_oracle_ingestion.sh");
		}
	}
}
