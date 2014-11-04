package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReportForwarderThread;

public class VertexConsumptionReporter extends AbstractVertexQosReporter {

	public VertexConsumptionReporter(QosReportForwarderThread reportForwarder,
			QosReporterID.Vertex reporterID, int runtimeInputGateIndex,
			InputGateReceiveCounter igReceiveCounter) {

		super(reportForwarder, reporterID, new ReportTimer(reportForwarder
				.getConfigCenter().getAggregationInterval()),
				runtimeInputGateIndex, -1, igReceiveCounter, null);
	}

	@Override
	public void recordReceived(int runtimeInputGateIndex) {
		if (canSendReport()) {
			long now = System.currentTimeMillis();
			sendReport(now, null);
		}
	}

	@Override
	public void tryingToReadRecord(int runtimeInputGateIndex) {
		// do nothing
	}

	@Override
	public void recordEmitted(int runtimeOutputGateIndex) {
		throw new RuntimeException("Method not implemented.");
	}
}
