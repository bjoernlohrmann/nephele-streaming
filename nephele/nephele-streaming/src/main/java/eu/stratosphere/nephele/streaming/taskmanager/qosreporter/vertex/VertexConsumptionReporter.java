package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReportForwarderThread;

public class VertexConsumptionReporter extends VertexQosReporter {
	//TODO implement
	public VertexConsumptionReporter(QosReportForwarderThread reportForwarder, QosReporterID.Vertex reporterID, int
			runtimeInputGateIndex, int runtimeOutputGateIndex) {
		super(reportForwarder, reporterID, runtimeInputGateIndex, runtimeOutputGateIndex);
	}

	@Override
	public void recordReceived(int runtimeInputGateIndex) {

	}

	@Override
	public void tryingToReadRecord(int runtimeInputGateIndex) {

	}

	@Override
	public void recordEmitted(int runtimeOutputGateIndex) {

	}
}
