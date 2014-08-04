package eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers;

/**
 * Models an entry in a time-series of values.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class HistoryEntry<T> {
	private int entryIndex;

	private long timestamp;

	private T value;

	public HistoryEntry(int entryIndex, long timestamp, T bufferSize) {
		this.entryIndex = entryIndex;
		this.timestamp = timestamp;
		this.value = bufferSize;
	}

	public int getEntryIndex() {
		return this.entryIndex;
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	public T getValue() {
		return this.value;
	}

}
