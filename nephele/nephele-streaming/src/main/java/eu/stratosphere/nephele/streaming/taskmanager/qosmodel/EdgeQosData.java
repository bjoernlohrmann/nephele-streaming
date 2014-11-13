package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import eu.stratosphere.nephele.streaming.message.qosreport.EdgeStatistics;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.ValueHistory;
import eu.stratosphere.nephele.streaming.util.StreamPluginConfig;

/**
 * Instances of this class hold Qos data (latency, throughput, ...) of a
 * {@link QosEdge}.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class EdgeQosData {

	private QosEdge edge;

	private QosStatistic latencyInMillisStatistic;

	private QosStatistic throughputInMbitStatistic;

	private QosStatistic outputBufferLifetimeStatistic;

	private QosStatistic recordsPerBufferStatistic;

	private QosStatistic recordsPerSecondStatistic;

	private boolean isInChain;
	
	private ValueHistory<Integer> targetOblHistory;


	public EdgeQosData(QosEdge edge) {
		this.edge = edge;
		this.isInChain = false;
		this.latencyInMillisStatistic = new QosStatistic(StreamPluginConfig.computeQosStatisticWindowSize());
		this.throughputInMbitStatistic = new QosStatistic(StreamPluginConfig.computeQosStatisticWindowSize());
		this.outputBufferLifetimeStatistic = new QosStatistic(
				StreamPluginConfig.computeQosStatisticWindowSize());
		this.recordsPerBufferStatistic = new QosStatistic(StreamPluginConfig.computeQosStatisticWindowSize());
		this.recordsPerSecondStatistic = new QosStatistic(StreamPluginConfig.computeQosStatisticWindowSize());
		this.targetOblHistory = new ValueHistory<Integer>(2);
	}

	public QosEdge getEdge() {
		return this.edge;
	}
	
	public double estimateOutputBufferLatencyInMillis() {
		double channelLatency = getChannelLatencyInMillis();
		double oblt = getOutputBufferLifetimeInMillis();

		if (channelLatency == -1 || oblt == -1) {
			return -1;
		}

		return Math.min(channelLatency, oblt / 2);
	}

	public double estimateTransportLatencyInMillis() {
		double channelLatency = getChannelLatencyInMillis();
		double oblt = getOutputBufferLifetimeInMillis();

		if (channelLatency == -1 || oblt == -1) {
			return -1;
		}

		return Math.max(0, channelLatency - (oblt / 2));
	}

	public double getChannelLatencyInMillis() {
		if (this.latencyInMillisStatistic.hasValues()) {
			return this.latencyInMillisStatistic.getMean();
		}

		return -1;
	}

	public double getChannelThroughputInMbit() {
		if (this.throughputInMbitStatistic.hasValues()) {
			return this.throughputInMbitStatistic.getMean();
		}
		return -1;
	}

	public double getOutputBufferLifetimeInMillis() {
		if (isInChain) {
			return 0;
		}
		if (this.outputBufferLifetimeStatistic.hasValues()) {
			return this.outputBufferLifetimeStatistic.getMean();
		}
		return -1;
	}

	public double getRecordsPerBuffer() {
		if (this.recordsPerBufferStatistic.hasValues()) {
			return this.recordsPerBufferStatistic.getMean();
		}
		return -1;
	}

	public double getRecordsPerSecond() {
		if (this.recordsPerSecondStatistic.hasValues()) {
			return this.recordsPerSecondStatistic.getMean();
		}
		return -1;
	}

	public void addLatencyMeasurement(long timestamp, double latencyInMillis) {
		QosValue value = new QosValue(latencyInMillis, timestamp);
		this.latencyInMillisStatistic.addValue(value);
	}

	public void addOutputChannelStatisticsMeasurement(long timestamp,
			EdgeStatistics stats) {

		QosValue throughput = new QosValue(stats.getThroughput(), timestamp);
		this.throughputInMbitStatistic.addValue(throughput);

		QosValue outputBufferLifetime = new QosValue(
				stats.getOutputBufferLifetime(), timestamp);
		this.outputBufferLifetimeStatistic.addValue(outputBufferLifetime);

		QosValue recordsPerBuffer = new QosValue(stats.getRecordsPerBuffer(),
				timestamp);
		this.recordsPerBufferStatistic.addValue(recordsPerBuffer);

		QosValue recordsPerSecond = new QosValue(stats.getRecordsPerSecond(),
				timestamp);
		this.recordsPerSecondStatistic.addValue(recordsPerSecond);
	}

	public void setIsInChain(boolean isInChain) {
		this.isInChain = isInChain;
		outputBufferLifetimeStatistic.clear();
		recordsPerBufferStatistic.clear();
		recordsPerSecondStatistic.clear();
	}

	public boolean isInChain() {
		return this.isInChain;
	}
	
	private boolean isChannelLatencyNewerThan(long thresholdTimestamp) {
		return latencyInMillisStatistic.hasValues()
				&& latencyInMillisStatistic.getOldestValue().getTimestamp() >= thresholdTimestamp;
	}

	private boolean isOutputBufferLifetimeNewerThan(long thresholdTimestamp) {
		if (isInChain) {
			return true;
		}
		return outputBufferLifetimeStatistic.hasValues() 
				&& outputBufferLifetimeStatistic.getOldestValue().getTimestamp() >= thresholdTimestamp;
	}

	public boolean hasNewerData(long thresholdTimestamp) {
		return isChannelLatencyNewerThan(thresholdTimestamp)
			&& isOutputBufferLifetimeNewerThan(thresholdTimestamp);
	}
	
	public void dropOlderData(long thresholdTimestamp) {
		if (!isChannelLatencyNewerThan(thresholdTimestamp)) {
			latencyInMillisStatistic.clear();
		}

		if (!isInChain()
				&& !isOutputBufferLifetimeNewerThan(thresholdTimestamp)) {
			throughputInMbitStatistic.clear();
			outputBufferLifetimeStatistic.clear();
			recordsPerBufferStatistic.clear();
			recordsPerSecondStatistic.clear();
		}
	}
	
	public ValueHistory<Integer> getTargetOblHistory() {
		return this.targetOblHistory;
	}
}
