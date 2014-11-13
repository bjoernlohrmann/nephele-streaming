package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReportForwarderThread;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.sampling.Sample;

public class VertexConsumptionReporter extends AbstractVertexQosReporter {
	
	private final InputGateInterReadTimeSampler igInterReadSampler;
	
	public VertexConsumptionReporter(QosReportForwarderThread reportForwarder,
			QosReporterID.Vertex reporterID, int runtimeInputGateIndex,
			InputGateReceiveCounter igReceiveCounter) {

		super(reportForwarder, reporterID, new ReportTimer(reportForwarder
				.getConfigCenter().getAggregationInterval()),
				runtimeInputGateIndex, -1, igReceiveCounter, null);
		
		igInterReadSampler = new InputGateInterReadTimeSampler(reportForwarder.getConfigCenter().getSamplingProbability() / 100.0);
	}

	@Override
	public void recordReceived(int runtimeInputGateIndex) {
		if (runtimeInputGateIndex == getRuntimeInputGateIndex()) {
			igInterReadSampler.recordReceivedOnIg();
			
			if (igInterReadSampler.hasSample() && canSendReport()) {
				long now = System.currentTimeMillis();
				Sample readReadTime = igInterReadSampler.drawSampleAndReset(now).rescale(0.001);
				sendReport(now, readReadTime);
			}
		}
	}

	@Override
	public void tryingToReadRecord(int runtimeInputGateIndex) {
		igInterReadSampler.tryingToReadRecordFromAnyIg();
	}

	@Override
	public void recordEmitted(int runtimeOutputGateIndex) {
		throw new RuntimeException("Method not implemented.");
	}
}
