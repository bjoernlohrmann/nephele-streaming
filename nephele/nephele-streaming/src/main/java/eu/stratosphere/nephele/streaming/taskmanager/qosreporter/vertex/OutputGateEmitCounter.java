package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

public class OutputGateEmitCounter {
	private int emitted;

	public void emitted() {
		emitted++;
	}

	public int getEmitted() {
		return emitted;
	}

	public void reset() {
		emitted = 0;
	}
}
