package com.amazonaws.streamdataingestion;

import java.util.List;

public interface IKinesisProducer {
	void putRecord(String record);

	void putRecords(List<String> records);
}
