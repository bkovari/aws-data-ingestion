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
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.StreamDescription;

public class KinesisProducer implements IKinesisProducer {

	private String _region;
	private String _name;
	private AWSCredentials _credentials;
	private static AmazonKinesis _producerClient;
	private static String _sequenceNumberOfPreviousRecord;
	private static Logger _logger = LoggerFactory.getLogger(KinesisProducer.class);

	public KinesisProducer(String region, String name) {
		_region = region;
		_name = name;
		setCredentials("default");
		setClient();
		_sequenceNumberOfPreviousRecord = "";
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
	public void putRecord(String record) {

		long createTime = System.currentTimeMillis();

		PutRecordRequest putRecordRequest = new PutRecordRequest();
		putRecordRequest.setStreamName(_name);
		putRecordRequest.setData(ByteBuffer.wrap(record.getBytes()));
		putRecordRequest.setPartitionKey(String.format("partitionKey-%d", createTime));

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

		long createTime = System.currentTimeMillis();

		PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
		putRecordsRequest.setStreamName(_name);
		List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
		for (String record : records) {
			PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
			putRecordsRequestEntry.setData(ByteBuffer.wrap(String.valueOf(record).getBytes()));
			putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", createTime));
			putRecordsRequestEntryList.add(putRecordsRequestEntry);
		}
		putRecordsRequest.setRecords(putRecordsRequestEntryList);
		PutRecordsResult putRecordsResult = _producerClient.putRecords(putRecordsRequest);

		// _logger.info("Put records result : {}", putRecordsResult);

	}

}
