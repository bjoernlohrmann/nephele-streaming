package eu.stratosphere.nephele.streaming.taskmanager.qosreporter;

import eu.stratosphere.nephele.streaming.SamplingStrategy;
import eu.stratosphere.nephele.streaming.message.qosreport.VertexStatistics;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Handles the measurement and reporting of latencies and record
 * consumption/emission rates for a particular vertex. Such a latency is defined
 * as the timespan between record receptions and emits on a particular
 * input/output gate combination of the vertex. Thus one vertex may have
 * multiple associated latencies, one for each input/output gate combination.
 * Which gate combination is measured and reported on must be configured by
 * calling {@link #addReporter(int, int, eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID.Vertex,
 * eu.stratosphere.nephele.streaming.SamplingStrategy)}.
 * <p/>
 * An {@link VertexStatistics} record per configured input/output gate
 * combination will be handed to the provided {@link QosReportForwarderThread}
 * approximately once per aggregation interval (see
 * {@link QosReporterConfigCenter}). "Approximately" because if no records have
 * been received/emitted, nothing will be reported.
 *
 * @author Bjoern Lohrmann
 */
public class VertexStatisticsReportManager {

	// private static final Log LOG =
	// LogFactory.getLog(TaskLatencyReporter.class);

	private QosReportForwarderThread reportForwarder;

	private ConcurrentHashMap<QosReporterID, VertexQosReporter> reporters;

	private AtomicReferenceArray<VertexQosReporter[]> reportersByInputGate;

	private AtomicReferenceArray<VertexQosReporter[]> reportersByOutputGate;

	private class VertexQosReporter {

		private final QosReporterID.Vertex reporterID;

		private final int runtimeInputGateIndex;

		private final int runtimeOutputGateIndex;

		private SamplingStrategy samplingStrategy;

		private int inputGateReceiveCounter;

		private int outputGateEmitCounter;

		private int reportingProbeInterval;

		private int currentReportingProbeCounter;

		private long timeOfNextReport;

		private long timeOfLastReport;

		private long amountSamples;

		private long sampleTimes;

		private long lastSampleReadTime;

		private SamplingManager samplingManager;

		public VertexQosReporter(QosReporterID.Vertex reporterID, int runtimeInputGateIndex, int runtimeOutputGateIndex,
				SamplingStrategy samplingStrategy) {
			this.reporterID = reporterID;
			this.runtimeInputGateIndex = runtimeInputGateIndex;
			this.runtimeOutputGateIndex = runtimeOutputGateIndex;
			this.samplingStrategy = samplingStrategy;
			this.samplingManager = new SamplingManager(
					VertexStatisticsReportManager.this.reportForwarder.getConfigCenter().getSamplingProbability() / 100.0);

			this.currentReportingProbeCounter = 0;
			this.reportingProbeInterval = 1;
			setTimeOfReports(System.currentTimeMillis());
		}

		private void setTimeOfReports(long now) {
			this.timeOfLastReport = now;
			this.timeOfNextReport = timeOfLastReport
					+ reportForwarder.getConfigCenter()
					.getAggregationInterval();
		}

		public void sendReportIfDue() {
			this.currentReportingProbeCounter++;
			if (this.currentReportingProbeCounter >= this.reportingProbeInterval) {
				this.currentReportingProbeCounter = 0;

				if (this.hasData()) {

					long now = System.currentTimeMillis();
					if (now >= this.timeOfNextReport) {

						double secsPassed = (now - timeOfLastReport) / 1000.0;

						double consumptionRate = runtimeInputGateIndex != -1 ? inputGateReceiveCounter / secsPassed : -1;
						double emissionRate = runtimeOutputGateIndex != -1 ? outputGateEmitCounter / secsPassed : -1;

						VertexStatisticsReportManager.this.reportForwarder
								.addToNextReport(new VertexStatistics(
										this.reporterID,
										sampleTimes / (double) amountSamples,
										consumptionRate,
										emissionRate));

						this.prepareNextReport(now);
					}
				}
			}
		}

		private void prepareNextReport(long now) {
			// try to probe 10 times per measurement interval
			this.reportingProbeInterval = (int) Math
					.ceil((inputGateReceiveCounter + outputGateEmitCounter) / 10.0);

			this.inputGateReceiveCounter = 0;
			this.outputGateEmitCounter = 0;
			this.amountSamples = 0;
			this.sampleTimes = 0;
			this.lastSampleReadTime = 0;
			this.samplingManager.reset();

			setTimeOfReports(now);
		}

		public boolean hasData() {
			return amountSamples > 0;
		}

		public void recordReceived(int runtimeInputGateIndex) {
			if (runtimeInputGateIndex == this.runtimeInputGateIndex) {
				this.inputGateReceiveCounter++;

				if (lastSampleReadTime == 0) {
					lastSampleReadTime = System.currentTimeMillis();
					return;
				}
			}

			if (samplingStrategy == SamplingStrategy.READ_READ && lastSampleReadTime != 0) {
				// we have a viable latency measurement but should we sample it?
				if (samplingManager.shouldSample()) {
					sample();
				}
				lastSampleReadTime = 0;
			}

			this.sendReportIfDue();
		}

		public void recordEmitted(int runtimeOutputGateIndex) {
			if (runtimeOutputGateIndex == this.runtimeOutputGateIndex) {
				this.outputGateEmitCounter++;

				if (samplingStrategy == SamplingStrategy.READ_WRITE && lastSampleReadTime != 0) {
					// we have a viable latency measurement but should we sample it?
					if (samplingManager.shouldSample()) {
						sample();
					}
					lastSampleReadTime = 0;
				}
			} else if (samplingStrategy == SamplingStrategy.READ_WRITE) {
				// reset sample when emitting from another output gate
				lastSampleReadTime = 0;
			}

			this.sendReportIfDue();
		}

		private void sample() {
			amountSamples++;
			sampleTimes += System.currentTimeMillis() - lastSampleReadTime;
		}
	}

	public VertexStatisticsReportManager(QosReportForwarderThread qosReporter,
			int noOfInputGates, int noOfOutputGates) {

		this.reportForwarder = qosReporter;
		this.reportersByInputGate = new AtomicReferenceArray<VertexQosReporter[]>(
				noOfInputGates);
		this.fillWithEmptyArrays(this.reportersByInputGate, noOfInputGates);
		this.reportersByOutputGate = new AtomicReferenceArray<VertexQosReporter[]>(
				noOfOutputGates);
		this.fillWithEmptyArrays(this.reportersByOutputGate, noOfOutputGates);
		this.reporters = new ConcurrentHashMap<QosReporterID, VertexQosReporter>();
	}

	private void fillWithEmptyArrays(
			AtomicReferenceArray<VertexQosReporter[]> reporterArrays,
			int noOfEmptyArrays) {

		VertexQosReporter[] emptyArray = new VertexQosReporter[0];
		for (int i = 0; i < noOfEmptyArrays; i++) {
			reporterArrays.set(i, emptyArray);
		}
	}

	public void recordReceived(int runtimeInputGateIndex) {
		for (VertexQosReporter reporter : this.reportersByInputGate
				.get(runtimeInputGateIndex)) {
			reporter.recordReceived(runtimeInputGateIndex);
		}
	}

	public void recordEmitted(int runtimeOutputGateIndex) {
		for (VertexQosReporter reporter : this.reportersByOutputGate
				.get(runtimeOutputGateIndex)) {
			reporter.recordEmitted(runtimeOutputGateIndex);
		}
	}

	public boolean containsReporter(QosReporterID.Vertex reporterID) {
		return this.reporters.containsKey(reporterID);
	}

	public synchronized void addReporter(int runtimeInputGateIndex,
			int runtimeOutputGateIndex, QosReporterID.Vertex reporterID, SamplingStrategy samplingStrategy) {

		if (this.reporters.containsKey(reporterID)) {
			return;
		}

		VertexQosReporter reporter = new VertexQosReporter(reporterID, runtimeInputGateIndex,
				runtimeOutputGateIndex, samplingStrategy);

		this.reporters.put(reporterID, reporter);

		if (runtimeInputGateIndex != -1) {
			// for the READ_READ as well as READ_WRITE strategy the reporter needs to keep track of all input gates
			for (int i = 0; i < this.reportersByInputGate.length(); i++) {
				this.appendReporterToArrayAt(this.reportersByInputGate, i, reporter);
			}
		}

		if (runtimeOutputGateIndex != -1) {
			if (samplingStrategy == SamplingStrategy.READ_WRITE) {
				// for the READ_WRITE strategy the reporter needs to keep track of all output gates, too
				for (int i = 0; i < this.reportersByOutputGate.length(); i++) {
					this.appendReporterToArrayAt(this.reportersByOutputGate, i, reporter);
				}
			} else {
				this.appendReporterToArrayAt(this.reportersByOutputGate, runtimeOutputGateIndex, reporter);
			}
		}
	}

	private void appendReporterToArrayAt(
			AtomicReferenceArray<VertexQosReporter[]> reporters, int index,
			VertexQosReporter reporterToAppend) {

		VertexQosReporter[] oldReporters = reporters.get(index);
		VertexQosReporter[] newReporters = new VertexQosReporter[oldReporters.length + 1];
		System.arraycopy(oldReporters, 0, newReporters, 0, oldReporters.length);
		newReporters[oldReporters.length] = reporterToAppend;
		reporters.set(index, newReporters);
	}
}
