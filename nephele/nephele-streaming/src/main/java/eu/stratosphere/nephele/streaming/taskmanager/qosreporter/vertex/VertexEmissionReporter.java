package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReportForwarderThread;

public class VertexEmissionReporter extends AbstractVertexQosReporter {

	public VertexEmissionReporter(QosReportForwarderThread reportForwarder,
			QosReporterID.Vertex reporterID, int runtimeOutputGateIndex,
			OutputGateEmitStatistics emitCounter) {

		super(reportForwarder, reporterID, new ReportTimer(reportForwarder
				.getConfigCenter().getAggregationInterval()), -1,
				runtimeOutputGateIndex, null, emitCounter);
	}

	@Override
	public void recordReceived(int runtimeInputGateIndex) {
		throw new RuntimeException(
				"Method should never be invoked. This is bug.");
	}

	@Override
	public void tryingToReadRecord(int runtimeInputGateIndex) {
		throw new RuntimeException(
				"Method should never be invoked. This is bug.");
	}

	@Override
	public void recordEmitted(int runtimeOutputGateIndex) {
		if (getReportTimer().reportIsDue()) {
			long now = System.currentTimeMillis();
			sendReport(now, null);
		}
	}
}
