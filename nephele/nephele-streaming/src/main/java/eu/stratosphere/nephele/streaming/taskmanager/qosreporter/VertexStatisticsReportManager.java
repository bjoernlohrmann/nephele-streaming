package eu.stratosphere.nephele.streaming.taskmanager.qosreporter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

import eu.stratosphere.nephele.streaming.message.qosreport.VertexStatistics;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;

/**
 * Handles the measurement and reporting of latencies and record
 * consumption/emission rates for a particular vertex. Such a latency is defined
 * as the timespan between record receptions and emits on a particular
 * input/output gate combination of the vertex. Thus one vertex may have
 * multiple associated latencies, one for each input/output gate combination.
 * Which gate combination is measured and reported on must be configured by
 * calling {@link #addReporterConfig(int, int, QosReporterID)}.
 * 
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

		private QosReporterID.Vertex reporterID;

		private int inputGateReceiveCounter;

		private int outputGateEmitCounter;

		private long inputGateTimeOfFirstReceive;

		private int reportingProbeInterval;

		private int currentReportingProbeCounter;

		private long timeOfNextReport;

		public VertexQosReporter(QosReporterID.Vertex reporterID) {
			this.reporterID = reporterID;
			this.reportingProbeInterval = VertexStatisticsReportManager.this.reportForwarder
					.getConfigCenter().getTaggingInterval();
		}

		public void sendReportIfDue() {
			this.currentReportingProbeCounter++;
			if (this.currentReportingProbeCounter >= this.reportingProbeInterval) {
				this.currentReportingProbeCounter = 0;
				if (this.hasData()) {
					long now = System.currentTimeMillis();

					if (now >= this.timeOfNextReport) {

						double avgLatencyPerReceivedRecord = (now - this.inputGateTimeOfFirstReceive)
								/ (1.0 * this.inputGateReceiveCounter);
						
						double secsPassed = (reportForwarder.getConfigCenter()
								.getAggregationInterval()
								+ now
								- this.timeOfNextReport) / 1000.0;
						
						double consumptionRate = inputGateReceiveCounter / secsPassed;
						double emissionRate = outputGateEmitCounter / secsPassed;

						VertexStatisticsReportManager.this.reportForwarder
								.addToNextReport(new VertexStatistics(
										this.reporterID,
										avgLatencyPerReceivedRecord,
										consumptionRate,
										emissionRate));

						this.prepareNextReport(now);
					}
				}
			}
		}

		private void prepareNextReport(long now) {
			this.inputGateReceiveCounter = 0;
			this.outputGateEmitCounter = 0;
			this.inputGateTimeOfFirstReceive = -1;
			this.timeOfNextReport = now
					+ reportForwarder.getConfigCenter().getAggregationInterval();
		}

		public boolean hasData() {
			return this.inputGateReceiveCounter > 0
					&& this.outputGateEmitCounter > 0;
		}

		public void recordReceived() {
			if (this.inputGateReceiveCounter == 0) {
				this.inputGateTimeOfFirstReceive = System.currentTimeMillis();
			}
			this.inputGateReceiveCounter++;
		}

		public void recordEmitted() {
			this.outputGateEmitCounter++;
			this.sendReportIfDue();
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
			reporter.recordReceived();
		}
	}

	public void recordEmitted(int runtimeOutputGateIndex) {
		for (VertexQosReporter reporter : this.reportersByOutputGate
				.get(runtimeOutputGateIndex)) {
			reporter.recordEmitted();
		}
	}

	public boolean containsReporter(QosReporterID.Vertex reporterID) {
		return this.reporters.containsKey(reporterID);
	}

	public synchronized void addReporter(int runtimeInputGateIndex,
			int runtimeOutputGateIndex, QosReporterID.Vertex reporterID) {

		if (this.reporters.containsKey(reporterID)) {
			return;
		}

		VertexQosReporter reporter = new VertexQosReporter(reporterID);

		this.reporters.put(reporterID, reporter);

		this.appendReporterToArrayAt(this.reportersByInputGate,
				runtimeInputGateIndex, reporter);
		this.appendReporterToArrayAt(this.reportersByOutputGate,
				runtimeOutputGateIndex, reporter);
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
