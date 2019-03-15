package com.amazonaws.streamdataingestion;

public class KinesisProducerFactory {

	/**
	 * Factory for producers.
	 * 
	 * @param region producer region
	 * @param name   stream name
	 * @return initialized producer
	 */
	public static KinesisProducer getInitializedProducer(String region, String name) {
		KinesisProducer producer = new KinesisProducer(region, name);
		producer.initExplicitHashKey();
		return producer;
	}
}
