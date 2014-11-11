package eu.stratosphere.nephele.streaming.taskmanager.qosreporter;

import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CopyOnWriteArrayList;

import eu.stratosphere.nephele.streaming.message.qosreport.EdgeLatency;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;

/**
 * A instance of this class keeps track of and reports on the latencies of an
 * input gate's input channels.
 * 
 * An {@link EdgeLatency} record per input channel will be handed to the
 * provided {@link QosReportForwarderThread} approximately once per aggregation
 * interval (see {@link QosReporterConfigCenter}). "Approximately" because if no
 * records have been received/emitted, nothing will be reported.
 * 
 * 
 * This class is thread-safe.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class InputGateReporterManager {

	/**
	 * No need for a thread-safe set because it is only accessed in synchronized
	 * methods.
	 */
	private HashSet<QosReporterID> reporters;

	/**
	 * Maps from an input channels index in the runtime gate to the latency
	 * reporter. This needs to be threadsafe because statistics collection may
	 * already be running while EdgeLatencyReporters are being added.
	 */
	private CopyOnWriteArrayList<EdgeLatencyReporter> reportersByChannelIndexInRuntimeGate;

	private QosReportForwarderThread reportForwarder;

	private class EdgeLatencyReporter {

		public QosReporterID.Edge reporterID;

		long timeOfNextReport;

		long accumulatedLatency;

		int tagsReceived;

		public void sendReportIfDue(long now) {
			if (this.reportIsDue(now)) {
				this.sendReport();
				this.reset(now);
			}
		}

		private void sendReport() {
			EdgeLatency channelLatency = new EdgeLatency(this.reporterID,
					this.accumulatedLatency / this.tagsReceived);
			InputGateReporterManager.this.reportForwarder
					.addToNextReport(channelLatency);
		}

		public boolean reportIsDue(long now) {
			return this.tagsReceived > 0 && now >= this.timeOfNextReport;
		}

		public void reset(long now) {
			this.timeOfNextReport = now
					+ InputGateReporterManager.this.reportForwarder
							.getConfigCenter().getAggregationInterval();
			this.accumulatedLatency = 0;
			this.tagsReceived = 0;
		}

		public void update(TimestampTag tag, long now) {
			// need to take max() because timestamp diffs can be below zero
			// due to clockdrift
			this.accumulatedLatency += Math.max(0, now - tag.getTimestamp());
			this.tagsReceived++;
		}
	}

	public InputGateReporterManager(QosReportForwarderThread qosReporter,
			int noOfInputChannels) {

		this.reportForwarder = qosReporter;
		this.reportersByChannelIndexInRuntimeGate = new CopyOnWriteArrayList<EdgeLatencyReporter>();
		this.fillChannelLatenciesWithNulls(noOfInputChannels);
		this.reporters = new HashSet<QosReporterID>();
	}

	private void fillChannelLatenciesWithNulls(int noOfInputChannels) {
		Collections.addAll(this.reportersByChannelIndexInRuntimeGate,
				new EdgeLatencyReporter[noOfInputChannels]);
	}

	public void reportLatencyIfNecessary(int channelIndex,
			TimestampTag timestampTag) {

		EdgeLatencyReporter info = this.reportersByChannelIndexInRuntimeGate
				.get(channelIndex);

		if (info != null) {
			long now = System.currentTimeMillis();
			info.update(timestampTag, now);
			info.sendReportIfDue(now);
		}
	}

	public synchronized boolean containsReporter(QosReporterID.Edge reporterID) {
		return this.reporters.contains(reporterID);
	}

	public synchronized void addEdgeQosReporterConfig(
			int channelIndexInRuntimeGate, QosReporterID.Edge reporterID) {

		if (this.reporters.contains(reporterID)) {
			return;
		}

		EdgeLatencyReporter info = new EdgeLatencyReporter();
		info.reporterID = reporterID;
		info.timeOfNextReport = System.currentTimeMillis();
		info.accumulatedLatency = 0;
		info.tagsReceived = 0;
		this.reportersByChannelIndexInRuntimeGate.set(
				channelIndexInRuntimeGate, info);
		this.reporters.add(reporterID);
	}
}
