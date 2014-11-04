package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;


public class InputGateReceiveCounter {

	private long recordsReceived;


	public void recordReceived() {
		recordsReceived++;
	}

	public long getRecordsReceived() {
		return recordsReceived;
	}

	public void reset() {
		recordsReceived = 0;
	}
}
