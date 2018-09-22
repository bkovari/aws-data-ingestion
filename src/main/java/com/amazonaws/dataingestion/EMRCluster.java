package com.amazonaws.dataingestion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceAsyncClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.Tag;

public class EMRCluster implements IEMRClusterConfig {

	private String name;
	private String region;
	private AWSCredentials credentials;
	private AmazonElasticMapReduce emrClient;
	private List<InstanceGroupConfig> instanceGroupConfigs;
	private List<Application> instanceApplications;
	private String releaseLabel;
	private BootstrapActionConfig clusterBootstrapConfig;
	private RunJobFlowRequest clusterCreationRequest;
	private RunJobFlowResult clusterCreationResult;

	public EMRCluster(String name, String region) {

		this.name = name;
		this.region = region;
		setCredentials("default");
		setClient();
	}

	public String getName() {
		return name;
	}

	public String getRegion() {
		return region;
	}

	public AWSCredentials getCredentials() {
		return credentials;
	}

	public AmazonElasticMapReduce getEmrClient() {
		return emrClient;
	}

	public List<InstanceGroupConfig> getInstanceGroupConfigs() {
		return instanceGroupConfigs;
	}

	public List<Application> getInstanceApplications() {
		return instanceApplications;
	}

	public String getReleaseLabel() {
		return releaseLabel;
	}

	public BootstrapActionConfig getClusterBootstrapConfig() {
		return clusterBootstrapConfig;
	}

	public RunJobFlowRequest getClusterCreationRequest() {
		return clusterCreationRequest;
	}

	public RunJobFlowResult getClusterCreationResult() {
		return clusterCreationResult;
	}

	public void setCredentials(String profileName) {

		try {
			credentials = new ProfileCredentialsProvider(profileName).getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load user credentials!", e);
		}
	}

	private void setClient() {

		try {
			emrClient = AmazonElasticMapReduceAsyncClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(region).build();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot create client with credentials and region!");
		}
	}

	public void setApplications(String releaseLabel, List<String> applications) {

		this.releaseLabel = releaseLabel;
		instanceApplications = new ArrayList<Application>();
		for (String app : applications) {
			instanceApplications.add(new Application().withName(app));
		}
	}

	public void setBootstrapConfig(String s3Path) {

		clusterBootstrapConfig = new BootstrapActionConfig().withName("Bootstrap action before Hadoop starts")
				.withScriptBootstrapAction(new ScriptBootstrapActionConfig().withPath(s3Path));
	}

	public void setMasterNode(String instanceType) {

		if (instanceGroupConfigs == null) {
			instanceGroupConfigs = new ArrayList<InstanceGroupConfig>();
		}
		instanceGroupConfigs.add(new InstanceGroupConfig().withInstanceCount(1).withInstanceRole("MASTER")
				.withInstanceType(instanceType).withMarket("ON_DEMAND"));
	}

	public void setCoreNode(String instanceType, Integer instanceCount) {
		if (instanceGroupConfigs == null) {
			instanceGroupConfigs = new ArrayList<InstanceGroupConfig>();
		}
		instanceGroupConfigs.add(new InstanceGroupConfig().withInstanceCount(instanceCount).withInstanceRole("CORE")
				.withInstanceType(instanceType).withMarket("ON_DEMAND"));
	}

	public void start(boolean enableDebugging) {

		clusterCreationRequest = new RunJobFlowRequest().withName(name).withReleaseLabel(releaseLabel)
				.withLogUri("s3://awsdataingestion/log").withApplications(instanceApplications)
				.withServiceRole("EMR_DefaultRole").withJobFlowRole("EMR_EC2_DefaultRole")
				.withBootstrapActions(clusterBootstrapConfig)
				.withInstances(new JobFlowInstancesConfig().withInstanceGroups(instanceGroupConfigs)
						.withEc2KeyName("MyEC2Master").withKeepJobFlowAliveWhenNoSteps(true))
				.withTags(new Tag().withKey("EMR").withValue("DataIngestion"));

		if (enableDebugging) {
			final String COMMAND_RUNNER = "command-runner.jar";
			final String DEBUGGING_COMMAND = "state-pusher-script";
			final String DEBUGGING_NAME = "Setup Hadoop Debugging";

			StepConfig debugging = new StepConfig().withName(DEBUGGING_NAME)
					.withActionOnFailure(ActionOnFailure.TERMINATE_CLUSTER)
					.withHadoopJarStep(new HadoopJarStepConfig().withJar(COMMAND_RUNNER).withArgs(DEBUGGING_COMMAND));

			clusterCreationRequest = clusterCreationRequest.withSteps(debugging);
		}

		System.out.println("Starting cluster: {" + name + "}" + " in region: {" + region + "}");
		clusterCreationResult = emrClient.runJobFlow(clusterCreationRequest);
		System.out.println("Request state: " + clusterCreationResult.getSdkResponseMetadata().toString());

	}

	public void stop() {
		emrClient.shutdown();
	}

	public void addClusterJob(String path, String args) {

	}

	public static void main(String[] args) {

		EMRCluster cluster = new EMRCluster("MyAWSCluster", "us-west-1");
		cluster.setApplications("emr-5.17.0", Arrays.asList("Hadoop", "Hive", "Hue", "Sqoop", "Oozie"));
		cluster.setBootstrapConfig("s3://awsdataingestion/scripts/bootstrap.sh");
		cluster.setMasterNode("m1.large");
		cluster.setCoreNode("m1.medium", 2);
		cluster.start(true);

	}

}
