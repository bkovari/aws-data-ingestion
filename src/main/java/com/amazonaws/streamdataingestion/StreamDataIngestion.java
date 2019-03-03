package com.amazonaws.streamdataingestion;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StreamDataIngestion implements Runnable {

	private static final String _kinesisStreamName = "MyKinesisDataStream";

	private static StreamDataIngestion _streamDataIngestionApplication;
	private static KinesisProducer _kinesisProducer;
	private static long _ingestionInterval;

	public StreamDataIngestion(long interval) {
		_ingestionInterval = interval;
	}

	public static void main(String[] args) {

		_streamDataIngestionApplication = new StreamDataIngestion(1000);
		_kinesisProducer = new KinesisProducer("eu-central-1", _kinesisStreamName);

		if (KinesisProducer.isStreamExists(_kinesisProducer.getName())) {
			ScheduledExecutorService ex = Executors.newSingleThreadScheduledExecutor();
			ex.scheduleAtFixedRate(_streamDataIngestionApplication, 0, _ingestionInterval, TimeUnit.MILLISECONDS);
		}

	}

	@Override
	public void run() {
		String record = StreamingUtils.generateWebserverLogEntry();
		_kinesisProducer.putRecord(record);

	}
}
