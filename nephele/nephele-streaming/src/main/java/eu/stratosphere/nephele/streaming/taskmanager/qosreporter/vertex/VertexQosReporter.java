package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

import eu.stratosphere.nephele.streaming.message.qosreport.VertexStatistics;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.BernoulliSampleDesign;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReportForwarderThread;

public abstract class VertexQosReporter {
	private QosReportForwarderThread reportForwarder;
	private QosReporterID.Vertex reporterID;
	private int runtimeInputGateIndex;
	private int runtimeOutputGateIndex;

	private int reportingProbeInterval;
	private int currentReportingProbeCounter;
	private long timeOfNextReport;
	private long timeOfLastReport;

	private long amountSamples;
	private long sampleTimes;
	private BernoulliSampleDesign bernoulliSampleDesign;
	protected InputGateReceiveCounter inputGateReceiveCounter;
	protected OutputGateEmitCounter outputGateEmitCounter;

	protected VertexQosReporter(QosReportForwarderThread reportForwarder, QosReporterID.Vertex reporterID,
			int runtimeInputGateIndex, int runtimeOutputGateIndex) {
		this.reportForwarder = reportForwarder;
		this.reporterID = reporterID;
		this.runtimeInputGateIndex = runtimeInputGateIndex;
		this.runtimeOutputGateIndex = runtimeOutputGateIndex;
		this.bernoulliSampleDesign =
				new BernoulliSampleDesign(reportForwarder.getConfigCenter().getSamplingProbability() / 100.0);

		this.currentReportingProbeCounter = 0;
		this.reportingProbeInterval = 1;
		setTimeOfReports(System.currentTimeMillis());

		this.inputGateReceiveCounter = new InputGateReceiveCounter();
		this.outputGateEmitCounter = new OutputGateEmitCounter();
	}

	private void setTimeOfReports(long now) {
		this.timeOfLastReport = now;
		this.timeOfNextReport = timeOfLastReport + reportForwarder.getConfigCenter().getAggregationInterval();
	}

	public QosReporterID.Vertex getReporterID() {
		return reporterID;
	}

	protected void addSample(long sampleTime) {
		amountSamples++;
		sampleTimes += sampleTime;
	}

	protected void sendReportIfDue() {
		sendReportIfDue(new ReportSendingStrategy() {
			@Override
			public void sendReport(QosReportForwarderThread reportForwarder, double avgSampleTime,
					double consumptionRate, double emissionRate) {
				reportForwarder.addToNextReport(new VertexStatistics(reporterID, avgSampleTime,
						consumptionRate, emissionRate));
			}
		});
	}

	protected void sendReportIfDue(ReportSendingStrategy sendingStrategy) {
		this.currentReportingProbeCounter++;
		if (this.currentReportingProbeCounter >= this.reportingProbeInterval) {
			this.currentReportingProbeCounter = 0;

			if (amountSamples > 0) {

				long now = System.currentTimeMillis();
				if (now >= this.timeOfNextReport) {

					double secsPassed = (now - timeOfLastReport) / 1000.0;

					double consumptionRate =
							runtimeInputGateIndex != -1 ? inputGateReceiveCounter.getReceived() / secsPassed : -1;
					double emissionRate = runtimeOutputGateIndex != -1 ? outputGateEmitCounter.getEmitted() / secsPassed : -1;

					sendingStrategy.sendReport(reportForwarder, sampleTimes / (double) amountSamples,
							consumptionRate, emissionRate);

					this.prepareNextReport(now);
				}
			}
		}
	}

	protected void prepareNextReport(long now) {
		// try to probe 10 times per measurement interval
		this.reportingProbeInterval =
				(int) Math.ceil((inputGateReceiveCounter.getReceived() + outputGateEmitCounter.getEmitted()) / 10.0);
		this.inputGateReceiveCounter.reset();
		this.outputGateEmitCounter.reset();
		this.amountSamples = 0;
		this.sampleTimes = 0;
		this.bernoulliSampleDesign.reset();

		setTimeOfReports(now);
	}

	protected boolean shouldSample() {
		return bernoulliSampleDesign.shouldSample();
	}

	public abstract void recordReceived(int runtimeInputGateIndex);

	public abstract void tryingToReadRecord(int runtimeInputGateIndex);

	public abstract void recordEmitted(int runtimeOutputGateIndex);

	public int getRuntimeInputGateIndex() {
		return runtimeInputGateIndex;
	}

	public int getRuntimeOutputGateIndex() {
		return runtimeOutputGateIndex;
	}
}
