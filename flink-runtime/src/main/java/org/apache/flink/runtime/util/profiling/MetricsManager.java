package org.apache.flink.runtime.util.profiling;

public interface MetricsManager {
	void updateTaskId(String taskDescription, Integer idInModel);

	void newInputBuffer(long timestamp);

	String getJobVertexId();

	void addSerialization(long serializationDuration);

	void incRecordsOut();

	void incRecordsOutKeyGroup(int targetKeyGroup);

	void incRecordIn(int keyGroup);

	void addWaitingForWriteBufferDuration(long duration);

	void inputBufferConsumed(long timestamp, long deserializationDuration, long processing, long numRecords, long endToEndLatency);

	void groundTruth(int keyGroup, long arrivalTs, long completionTs);

	void outputBufferFull(long timestamp);

	void updateMetrics();
}
