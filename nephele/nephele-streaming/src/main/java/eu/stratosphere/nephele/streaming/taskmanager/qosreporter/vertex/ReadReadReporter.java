package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReportForwarderThread;

public class ReadReadReporter extends VertexQosReporter {
	private long lastSampleReadTime;
	private boolean retrySample;

	public ReadReadReporter(QosReportForwarderThread reportForwarder, QosReporterID.Vertex reporterID, int
			runtimeInputGateIndex, int runtimeOutputGateIndex) {
		super(reportForwarder, reporterID, runtimeInputGateIndex, runtimeOutputGateIndex);
	}

	@Override
	public void recordReceived(int runtimeInputGateIndex) {
		if (runtimeInputGateIndex == getRuntimeInputGateIndex()) {
			inputGateReceiveCounter.received();
			if (retrySample || shouldSample()) {
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
		if (lastSampleReadTime != 0 && retrySample) {
			// if lastSampleReadTime is set then we should sample
			addSample(System.currentTimeMillis() - lastSampleReadTime);
			lastSampleReadTime = 0;
			retrySample = false;
		}

		sendReportIfDue();
	}

	@Override
	public void recordEmitted(int runtimeOutputGateIndex) {
		if (runtimeOutputGateIndex == getRuntimeOutputGateIndex()) {
			outputGateEmitCounter.emitted();
		}

		sendReportIfDue();
	}
}
