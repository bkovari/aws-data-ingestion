package com.amazonaws.streamdataingestion;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.acmpca.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;

public class KinesisProducer implements IKinesisProducer {

	private String _region;
	private String _name;
	private AWSCredentials _credentials;
	private static AmazonKinesis _producerClient;
	private static String _sequenceNumberOfPreviousRecord;
	private static String _shardChangeKey;
	private static String _shardExplicitHashKey;
	private static Logger _logger = LoggerFactory.getLogger(KinesisProducer.class);

	public KinesisProducer(String region, String name) {
		_region = region;
		_name = name;
		setCredentials("default");
		setClient();
		_sequenceNumberOfPreviousRecord = "";
	}

	/**
	 * @return producer region
	 */
	public String getRegion() {
		return _region;
	}

	/**
	 * @return producer stream name
	 */
	public String getName() {
		return _name;
	}

	/**
	 * @param profileName AWS credentials profile name
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

	/**
	 * @return AWS credentials
	 */
	public AWSCredentials getCredentials() {
		return _credentials;
	}

	/**
	 * @param credentials AWS credentials
	 */
	public void setCredentials(AWSCredentials credentials) {
		_credentials = credentials;
	}

	/**
	 * @return producer client
	 */
	public static AmazonKinesis getProducerClient() {
		return _producerClient;
	}

	/**
	 * Returns whether the stream with specified name exists or not.
	 * 
	 * @param streamName stream to be checked
	 * @return boolean result
	 */
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
	public void putRecord(String record) {

		PutRecordRequest putRecordRequest = new PutRecordRequest();
		putRecordRequest.setStreamName(_name);
		putRecordRequest.setData(ByteBuffer.wrap(record.getBytes()));
		putRecordRequest.setPartitionKey("partitionKey");
		updateExplicitHashKey();
		putRecordRequest.setExplicitHashKey(_shardExplicitHashKey);

		if (_sequenceNumberOfPreviousRecord != "") {
			putRecordRequest.setSequenceNumberForOrdering(_sequenceNumberOfPreviousRecord);
		}

		PutRecordResult putRecordResult = _producerClient.putRecord(putRecordRequest);

		_logger.info("Successfully put record: {}, partition key : {}, ShardID : {}, SequenceNumber : {}", record,
				putRecordRequest.getPartitionKey(), putRecordResult.getShardId(), putRecordResult.getSequenceNumber());

		_sequenceNumberOfPreviousRecord = putRecordResult.getSequenceNumber();
	}

	@Override
	public void putRecords(List<String> records) {

		PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
		putRecordsRequest.setStreamName(_name);
		updateExplicitHashKey();
		List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
		for (String record : records) {
			PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
			putRecordsRequestEntry.setData(ByteBuffer.wrap(String.valueOf(record).getBytes()));
			putRecordsRequestEntry.setPartitionKey("partitionKey");
			putRecordsRequestEntry.setExplicitHashKey(_shardExplicitHashKey);
			putRecordsRequestEntryList.add(putRecordsRequestEntry);
		}
		putRecordsRequest.setRecords(putRecordsRequestEntryList);
		PutRecordsResult putRecordsResult = _producerClient.putRecords(putRecordsRequest);

		_logger.info("Put records result : {}", putRecordsResult.toString());

	}

	/**
	 * Initializes explicit hash key for round-robin record distribution in case of
	 * 2 shards.
	 */
	public void initExplicitHashKey() {

		ListShardsRequest listShardsRequest = new ListShardsRequest().withStreamName(_name);
		ListShardsResult listShardsResult = _producerClient.listShards(listShardsRequest);
		List<Shard> shards = listShardsResult.getShards();

		_shardChangeKey = shards.get(1).getHashKeyRange().getStartingHashKey();
		_shardExplicitHashKey = "0";

		_logger.info("Shard0 hash ranges: {}", shards.get(0).getHashKeyRange());
		_logger.info("Shard1 hash ranges: {}", shards.get(1).getHashKeyRange());
		_logger.info("Shard change key: {}", _shardChangeKey);
	}

	private void updateExplicitHashKey() {
		_shardExplicitHashKey = _shardExplicitHashKey == _shardChangeKey ? "0" : _shardChangeKey;
	}

}
