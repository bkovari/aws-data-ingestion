package com.amazonaws.streamdataingestion;

import java.util.List;

public interface IKinesisProducer {
	/**
	 * @param record record to be but into the stream
	 */
	void putRecord(String record);

	/**
	 * @param records list of records to be put into the stream
	 */
	void putRecords(List<String> records);
}
