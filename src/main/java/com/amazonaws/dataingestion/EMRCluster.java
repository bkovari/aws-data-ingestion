package com.amazonaws.dataingestion;

import java.util.Arrays;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.*;
import com.amazonaws.services.elasticmapreduce.model.*;


public class EMRCluster {
	
	public static void main(String[] args) {
		
		AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\Bence\\.aws\\credentials), and is in valid format.",
                    e);
        }
        AmazonElasticMapReduce emr = AmazonElasticMapReduceAsyncClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion("us-west-1")
            .build();
        
        
        List <InstanceGroupConfig> instanceGroupConfigs = Arrays.asList(
	        new InstanceGroupConfig().withInstanceCount(1).withInstanceRole("MASTER").withInstanceType("m1.large").withMarket("ON_DEMAND"),
	        new InstanceGroupConfig().withInstanceCount(3).withInstanceRole("CORE").withInstanceType("m1.medium").withMarket("ON_DEMAND")
        );  
        
        List <Application> instanceApplications = Arrays.asList(
        		new Application().withName("Hadoop"),
        		new Application().withName("Hive"),
        		new Application().withName("Hue"),
        		new Application().withName("Sqoop"),
        		new Application().withName("Pig"),
        		new Application().withName("Oozie")
        );
        
        BootstrapActionConfig ojdbcConnectorSetup = new BootstrapActionConfig()
        		.withName("Oracle JDBC 6.0 Connector Setup for Sqoop")
        		.withScriptBootstrapAction(new ScriptBootstrapActionConfig().withPath("s3://cluster-tools/ojdbc_setup.sh"));
        
       final String COMMAND_RUNNER = "command-runner.jar";
       final String DEBUGGING_COMMAND = "state-pusher-script";
       final String DEBUGGING_NAME = "Setup Hadoop Debugging";   

       StepConfig enableDebugging = new StepConfig()
           .withName(DEBUGGING_NAME)
           .withActionOnFailure(ActionOnFailure.TERMINATE_CLUSTER)
           .withHadoopJarStep(new HadoopJarStepConfig()
               .withJar(COMMAND_RUNNER)
               .withArgs(DEBUGGING_COMMAND));

       RunJobFlowRequest clusterCreationRequest = new RunJobFlowRequest()
           .withName("MyEMRCluster")
           .withReleaseLabel("emr-5.17.0")
           .withLogUri("s3://emr-cluster-logger")
           .withSteps(enableDebugging)
           .withApplications(instanceApplications)
           .withServiceRole("EMR_DefaultRole")
           .withJobFlowRole("EMR_EC2_DefaultRole")
           .withBootstrapActions(ojdbcConnectorSetup)
           .withInstances(new JobFlowInstancesConfig().withInstanceGroups(instanceGroupConfigs)
               .withEc2KeyName("MyEC2Master")
               .withKeepJobFlowAliveWhenNoSteps(true))
           .withTags(new Tag().withKey("EMR").withValue("DataIngestion"));
       

       RunJobFlowResult result = emr.runJobFlow(clusterCreationRequest);
    		    
	}

}
