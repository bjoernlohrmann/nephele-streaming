package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

public class OutputGateEmitStatistics {
	private long emitted;

	public void emitted() {
		emitted++;
	}

	public long getEmitted() {
		return emitted;
	}

	public void reset() {
		emitted = 0;
	}
}
