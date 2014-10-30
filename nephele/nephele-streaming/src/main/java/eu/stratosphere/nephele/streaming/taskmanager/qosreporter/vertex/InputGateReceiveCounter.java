package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

public class InputGateReceiveCounter {
	private int received;

	public void received() {
		received++;
	}

	public int getReceived() {
		return received;
	}

	public void reset() {
		received = 0;
	}
}
