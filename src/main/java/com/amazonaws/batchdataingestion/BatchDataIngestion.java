package com.amazonaws.batchdataingestion;

import java.util.Arrays;

public class BatchDataIngestion {

	private EMRCluster startCluster() {

		EMRCluster cluster = new EMRCluster("MyAWSCluster", "eu-central-1");
		cluster.setApplications("emr-5.17.0", Arrays.asList("Sqoop", "Hadoop", "Hive"));
		cluster.setBootstrapConfig("s3://batchdataingestion/scripts/bootstrap.sh");
		cluster.setMasterNode("m4.xlarge");
		cluster.setCoreNode("m4.large", 2);
		cluster.start(true);

		return cluster;

	}

	public static void main(String[] args) {

		BatchDataIngestion dataIngestionApplication = new BatchDataIngestion();
		EMRCluster cluster = dataIngestionApplication.startCluster();
		// cluster.addJob("Test", "s3://batchdataingestion/scripts/job.sh");
	}

}
