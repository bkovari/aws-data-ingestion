package com.amazonaws.streamdataingestion;

public interface IKinesisProducer {
	public void putRecord(String value);
}
