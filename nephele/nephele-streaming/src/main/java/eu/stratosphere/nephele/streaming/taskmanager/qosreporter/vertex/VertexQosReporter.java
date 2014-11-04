package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;


public interface VertexQosReporter {
	
	public void recordReceived(int runtimeInputGateIndex);

	public void tryingToReadRecord(int runtimeInputGateIndex);

	public void recordEmitted(int runtimeOutputGateIndex);
	
	public void inputBufferConsumed(int inputGateIndex, int channelIndex,
			long bufferInterarrivalTimeNanos, int recordsReadFromBuffer);

	public int getRuntimeInputGateIndex();

	public int getRuntimeOutputGateIndex();
	
	public ReportTimer getReportTimer();
}
