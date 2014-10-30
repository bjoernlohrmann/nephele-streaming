package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

import eu.stratosphere.nephele.streaming.message.qosreport.VertexStatistics;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReportForwarderThread;

import java.util.ArrayList;
import java.util.List;

public class ReadReadVertexQosReporterGroup extends VertexQosReporter {
	private long lastSampleReadTime;
	private boolean retrySample;
	private List<VertexQosReporter> reporters = new ArrayList<VertexQosReporter>();

	public ReadReadVertexQosReporterGroup(QosReportForwarderThread reportForwarder, QosReporterID.Vertex reporterID,
			int runtimeInputGateIndex, int runtimeOutputGateIndex) {
		super(reportForwarder, reporterID, runtimeInputGateIndex, runtimeOutputGateIndex);
	}

	public void addReporter(ReadReadReporter reporter) {
		reporters.add(reporter);
	}

	@Override
	public void sendReportIfDue() {
		sendReportIfDue(new ReportSendingStrategy() {
			@Override
			public void sendReport(QosReportForwarderThread reportForwarder, double avgSampleTime, double consumptionRate,
					double emissionRate) {
				for (VertexQosReporter reporter : reporters) {
					reportForwarder.addToNextReport(new VertexStatistics(reporter.getReporterID(), avgSampleTime,
							consumptionRate, emissionRate));
				}
			}
		});
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
		if (lastSampleReadTime != 0) {
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
