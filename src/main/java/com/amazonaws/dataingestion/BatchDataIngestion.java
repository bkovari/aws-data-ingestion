package com.amazonaws.dataingestion;

import java.util.Arrays;

public class BatchDataIngestion {

	private EMRCluster startCluster() {

		EMRCluster cluster = new EMRCluster("MyAWSCluster", "us-west-1");
		cluster.setApplications("emr-5.17.0", Arrays.asList("Sqoop"));
		// cluster.setApplications("emr-5.17.0", Arrays.asList("Hadoop", "Hive", "Hue",
		// "Sqoop", "Oozie"));
		cluster.setBootstrapConfig("s3://awsdataingestion/scripts/bootstrap.sh");
		cluster.setMasterNode("m1.large");
		cluster.setCoreNode("m1.medium", 2);
		cluster.start(true);

		return cluster;

	}

	public static void main(String[] args) {

		BatchDataIngestion dataIngestionApplication = new BatchDataIngestion();
		EMRCluster cluster = dataIngestionApplication.startCluster();
		cluster.addJob("Test", "s3://awsdataingestion/scripts/job.sh");

		System.out.println(EMRCluster.isClusterRunning("MyAWSCluster", "us-west-1"));

	}

}
