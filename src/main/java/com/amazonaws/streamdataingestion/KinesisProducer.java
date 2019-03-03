package com.amazonaws.streamdataingestion;

import java.nio.ByteBuffer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.acmpca.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.StreamDescription;

public class KinesisProducer implements IKinesisProducer {

	private String _region;
	private String _name;
	private AWSCredentials _credentials;
	private static AmazonKinesis _producerClient;

	public KinesisProducer(String region, String name) {
		_region = region;
		_name = name;
		setCredentials("default");
		setClient();
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

	private void setClient() {
		_producerClient = AmazonKinesisClientBuilder.standard()
				.withCredentials((new AWSStaticCredentialsProvider(_credentials))).withRegion(_region).build();
	}

	public String getRegion() {
		return _region;
	}

	public String getName() {
		return _name;
	}

	public AWSCredentials getCredentials() {
		return _credentials;
	}

	public void setCredentials(AWSCredentials credentials) {
		_credentials = credentials;
	}

	public static AmazonKinesis getProducerClient() {
		return _producerClient;
	}

	public static boolean isStreamExists(String streamName) {

		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName);
		try {
			StreamDescription streamDescription = _producerClient.describeStream(describeStreamRequest)
					.getStreamDescription();
			System.out.printf("Stream %s has a status of %s.\n", streamName, streamDescription.getStreamStatus());

		} catch (ResourceNotFoundException e) {
		}

		return true;

	}

	@Override
	public void putRecord(String value) {
		long createTime = System.currentTimeMillis();

		PutRecordRequest putRecordRequest = new PutRecordRequest();
		putRecordRequest.setStreamName(_name);
		putRecordRequest.setData(ByteBuffer.wrap(value.getBytes()));
		putRecordRequest.setPartitionKey(String.format("partitionKey-%d", createTime));

		PutRecordResult putRecordResult = _producerClient.putRecord(putRecordRequest);
		System.out.printf("Successfully put record: %s, partition key : %s, ShardID : %s, SequenceNumber : %s.\n",
				value, putRecordRequest.getPartitionKey(), putRecordResult.getShardId(),
				putRecordResult.getSequenceNumber());

	}

}
