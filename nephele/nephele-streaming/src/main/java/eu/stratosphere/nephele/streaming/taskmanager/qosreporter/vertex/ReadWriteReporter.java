package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReportForwarderThread;

public class ReadWriteReporter extends VertexQosReporter {
	private long lastSampleReadTime;
	private boolean retrySample;

	public ReadWriteReporter(QosReportForwarderThread reportForwarder, QosReporterID.Vertex reporterID, int
			runtimeInputGateIndex, int runtimeOutputGateIndex) {
		super(reportForwarder, reporterID, runtimeInputGateIndex, runtimeOutputGateIndex);
	}

	@Override
	public void recordReceived(int runtimeInputGateIndex) {
		if (runtimeInputGateIndex == getRuntimeInputGateIndex()) {
			inputGateReceiveCounter.received();

			if (lastSampleReadTime == 0 && (retrySample || shouldSample())) {
				lastSampleReadTime = System.currentTimeMillis();
				retrySample = true;
			}
		} else {
			lastSampleReadTime = 0;
		}

		sendReportIfDue();
	}

	@Override
	public void tryingToReadRecord(int runtimeInputGateIndex) {
		// nothing to do here
	}

	@Override
	public void recordEmitted(int runtimeOutputGateIndex) {
		if (runtimeOutputGateIndex == getRuntimeOutputGateIndex()) {
			outputGateEmitCounter.emitted();
			if (lastSampleReadTime != 0) {
				addSample(System.currentTimeMillis() - lastSampleReadTime);
				retrySample = false;
			}
		}
		lastSampleReadTime = 0;

		sendReportIfDue();
	}
}
