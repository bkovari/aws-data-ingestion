package com.amazonaws.batchdataingestion;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceAsyncClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.AmazonElasticMapReduceException;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.BootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.ClusterSummary;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.InstanceGroupConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.ScriptBootstrapActionConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.Tag;

/**
 * @author Bence
 *
 */
public class EMRCluster implements IEMRClusterConfig, IEMRCluster {

	private String _name;
	private String _region;
	private AWSCredentials _credentials;
	private AmazonElasticMapReduce _emrClient;
	private List<InstanceGroupConfig> _instanceGroupConfigs;
	private List<Application> _instanceApplications;
	private String _releaseLabel;
	private BootstrapActionConfig _clusterBootstrapConfig;
	private RunJobFlowRequest _clusterCreationRequest;
	private RunJobFlowResult _clusterCreationResult;
	private AddJobFlowStepsRequest _addJobRequest;
	private AddJobFlowStepsResult _addJobResult;

	/**
	 * @param name
	 * @param region
	 */
	public EMRCluster(String name, String region) {

		_name = name;
		_region = region;
		setCredentials("default");
		setClient();
	}

	/**
	 * @return
	 */
	public String getName() {
		return _name;
	}

	/**
	 * @return
	 */
	public String getRegion() {
		return _region;
	}

	/**
	 * @return
	 */
	public AWSCredentials getCredentials() {
		return _credentials;
	}

	/**
	 * @return
	 */
	public AmazonElasticMapReduce getEmrClient() {
		return _emrClient;
	}

	/**
	 * @return
	 */
	public List<InstanceGroupConfig> getInstanceGroupConfigs() {
		return _instanceGroupConfigs;
	}

	/**
	 * @return
	 */
	public List<Application> getInstanceApplications() {
		return _instanceApplications;
	}

	/**
	 * @return
	 */
	public String getReleaseLabel() {
		return _releaseLabel;
	}

	/**
	 * @return
	 */
	public BootstrapActionConfig getClusterBootstrapConfig() {
		return _clusterBootstrapConfig;
	}

	/**
	 * @return
	 */
	public RunJobFlowRequest getClusterCreationRequest() {
		return _clusterCreationRequest;
	}

	/**
	 * @return
	 */
	public RunJobFlowResult getClusterCreationResult() {
		return _clusterCreationResult;
	}

	/**
	 * @param profileName
	 */
	public void setCredentials(String profileName) {

		try {
			_credentials = new ProfileCredentialsProvider(profileName).getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load user credentials!", e);
		}
	}

	/**
	 * 
	 */
	private void setClient() {

		try {
			_emrClient = AmazonElasticMapReduceAsyncClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(_credentials)).withRegion(_region).build();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot create client with credentials and region!");
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.amazonaws.dataingestion.IEMRClusterConfig#setApplications(java.lang.
	 * String, java.util.List)
	 */
	public void setApplications(String releaseLabel, List<String> applications) {

		this._releaseLabel = releaseLabel;
		_instanceApplications = new ArrayList<Application>();
		for (String app : applications) {
			_instanceApplications.add(new Application().withName(app));
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.amazonaws.dataingestion.IEMRClusterConfig#setBootstrapConfig(java.lang.
	 * String)
	 */
	public void setBootstrapConfig(String s3Path) {

		_clusterBootstrapConfig = new BootstrapActionConfig().withName("Bootstrap action before Hadoop starts")
				.withScriptBootstrapAction(new ScriptBootstrapActionConfig().withPath(s3Path));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.amazonaws.dataingestion.IEMRClusterConfig#setMasterNode(java.lang.String)
	 */
	public void setMasterNode(String instanceType) {

		if (_instanceGroupConfigs == null) {
			_instanceGroupConfigs = new ArrayList<InstanceGroupConfig>();
		}
		_instanceGroupConfigs.add(new InstanceGroupConfig().withInstanceCount(1).withInstanceRole("MASTER")
				.withInstanceType(instanceType).withMarket("ON_DEMAND"));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.amazonaws.dataingestion.IEMRClusterConfig#setCoreNode(java.lang.String,
	 * java.lang.Integer)
	 */
	public void setCoreNode(String instanceType, Integer instanceCount) {
		if (_instanceGroupConfigs == null) {
			_instanceGroupConfigs = new ArrayList<InstanceGroupConfig>();
		}
		_instanceGroupConfigs.add(new InstanceGroupConfig().withInstanceCount(instanceCount).withInstanceRole("CORE")
				.withInstanceType(instanceType).withMarket("ON_DEMAND"));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.amazonaws.dataingestion.IEMRCluster#start(boolean)
	 */
	public void start(boolean enableDebugging) {

		_clusterCreationRequest = new RunJobFlowRequest().withName(_name).withReleaseLabel(_releaseLabel)
				.withLogUri("s3://batchdataingestion/log").withApplications(_instanceApplications)
				.withServiceRole("EMR_DefaultRole").withJobFlowRole("EMR_EC2_DefaultRole")
				.withBootstrapActions(_clusterBootstrapConfig)
				.withInstances(new JobFlowInstancesConfig().withInstanceGroups(_instanceGroupConfigs)
						.withEc2KeyName("MyFrankfurtKey").withKeepJobFlowAliveWhenNoSteps(true))
				.withTags(new Tag().withKey("EMR").withValue("DataIngestion"));

		if (enableDebugging) {
			final String COMMAND_RUNNER = "command-runner.jar";
			final String DEBUGGING_COMMAND = "state-pusher-script";
			final String DEBUGGING_NAME = "Setup Hadoop Debugging";

			StepConfig debugging = new StepConfig().withName(DEBUGGING_NAME)
					.withActionOnFailure(ActionOnFailure.TERMINATE_CLUSTER)
					.withHadoopJarStep(new HadoopJarStepConfig().withJar(COMMAND_RUNNER).withArgs(DEBUGGING_COMMAND));

			_clusterCreationRequest = _clusterCreationRequest.withSteps(debugging);
		}

		System.out.println("Starting cluster: {" + _name + "}" + " in region: {" + _region + "}");
		_clusterCreationResult = _emrClient.runJobFlow(_clusterCreationRequest);
		System.out.println("Request state: " + _clusterCreationResult.getSdkResponseMetadata().toString());

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.amazonaws.dataingestion.IEMRCluster#stop()
	 */
	public void stop() {
		_emrClient.shutdown();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.amazonaws.dataingestion.IEMRCluster#addClusterJob(java.lang.String,
	 * java.lang.String)
	 */
	public void addJob(String jobName, String path) {

		String jar = String.format("s3://%s.elasticmapreduce/libs/script-runner/script-runner.jar", _region);
		StepConfig job = new StepConfig().withName(jobName).withActionOnFailure(ActionOnFailure.CONTINUE)
				.withHadoopJarStep(new HadoopJarStepConfig().withJar(jar).withArgs(path));

		String jobFlowId = _clusterCreationResult.getJobFlowId();
		_addJobRequest = new AddJobFlowStepsRequest().withSteps(job).withJobFlowId(jobFlowId);

		System.out.println("Adding job {" + jobName + "} to cluster.. ");
		_addJobResult = _emrClient.addJobFlowSteps(_addJobRequest);
		System.out.println("Request state: + " + _addJobResult.getSdkResponseMetadata().toString());

	}

	/**
	 * @return
	 */
	public String getState() {

		try {
			String jobFlowId = _clusterCreationResult.getJobFlowId();
			DescribeClusterResult result = _emrClient
					.describeCluster(new DescribeClusterRequest().withClusterId(jobFlowId));
			String state = result.getCluster().getStatus().toString();
			return state;
		} catch (Exception e) {
			throw new AmazonElasticMapReduceException("Unable to retrieve cluster status!");
		}

	}

	/**
	 * @param name
	 * @param region
	 * @return
	 */
	public static boolean isClusterRunning(String name, String region) {

		EMRCluster emrCluster = new EMRCluster(name, region);
		AmazonElasticMapReduce client = emrCluster.getEmrClient();
		List<ClusterSummary> clusters = client.listClusters().getClusters();

		for (ClusterSummary cluster : clusters) {
			if (cluster.getName().contains(name) && cluster.getStatus().toString().contains("State: WAITING")) {
				return true;
			}
		}

		return false;

	}
}
