package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

/**
 * A basic reporter providing (record) counting methods.
 */
public class CountingGateReporter {
	private boolean reporter;
	private long recordsCount;

	public boolean isReporter() {
		return reporter;
	}

	public void setReporter(boolean reporter) {
		this.reporter = reporter;
	}

	public long getRecordsCount() {
		return recordsCount;
	}

	public void countRecord() {
		++recordsCount;
	}

	public void reset() {
		recordsCount = 0;
	}
}