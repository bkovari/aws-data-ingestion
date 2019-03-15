package com.amazonaws.streamdataingestion;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StreamDataIngestion implements Runnable {

	private static final int _putRecordsPerSecondCount = 500;
	private static final int _ingestionInterval = 500;
	private static final TimeUnit _timeUnit = TimeUnit.MILLISECONDS;

	private static StreamDataIngestion _streamDataIngestionApplication;
	private static KinesisProducer _producer;

	public static void main(String[] args) {

		_streamDataIngestionApplication = new StreamDataIngestion();
		_producer = KinesisProducerFactory.getInitializedProducer("eu-central-1", "MyKinesisDataStream");

		if (KinesisProducer.isStreamExists(_producer.getName())) {
			ScheduledExecutorService ex = Executors.newSingleThreadScheduledExecutor();
			ex.scheduleAtFixedRate(_streamDataIngestionApplication, 0, _ingestionInterval, _timeUnit);
		}
	}

	@Override
	public void run() {
		List<String> records = StreamingUtils.generateWebserverLogEntries(_putRecordsPerSecondCount);
		_producer.putRecords(records);
	}
}
