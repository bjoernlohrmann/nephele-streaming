package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.streaming.message.qosreport.EdgeStatistics;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.BufferSizeHistory;
import eu.stratosphere.nephele.taskmanager.bufferprovider.GlobalBufferPool;

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

	private BufferSizeHistory bufferSizeHistory;

	private final static int DEFAULT_NO_OF_STATISTICS_ENTRIES = 4;

	private boolean isInChain;
	
	private int targetOutputBufferLatency;

	public EdgeQosData(QosEdge edge, int noOfStatisticsEntries) {
		this.edge = edge;
		this.isInChain = false;
		this.latencyInMillisStatistic = new QosStatistic(noOfStatisticsEntries);
		this.throughputInMbitStatistic = new QosStatistic(noOfStatisticsEntries);
		this.outputBufferLifetimeStatistic = new QosStatistic(
				noOfStatisticsEntries);
		this.recordsPerBufferStatistic = new QosStatistic(noOfStatisticsEntries);
		this.recordsPerSecondStatistic = new QosStatistic(noOfStatisticsEntries);
		this.bufferSizeHistory = new BufferSizeHistory(2);
		this.bufferSizeHistory.addToHistory(System.currentTimeMillis(),
				GlobalConfiguration.getInteger(
						"channel.network.bufferSizeInBytes",
						GlobalBufferPool.DEFAULT_BUFFER_SIZE_IN_BYTES));
		
		this.targetOutputBufferLatency = -1; 
	}

	public EdgeQosData(QosEdge edge) {
		this(edge, DEFAULT_NO_OF_STATISTICS_ENTRIES);
	}

	public QosEdge getEdge() {
		return this.edge;
	}

	public double getChannelLatencyInMillis() {
		if (this.latencyInMillisStatistic.hasValues()) {
			return this.latencyInMillisStatistic.getArithmeticMean();
		}

		return -1;
	}

	public double getChannelThroughputInMbit() {
		if (this.throughputInMbitStatistic.hasValues()) {
			return this.throughputInMbitStatistic.getArithmeticMean();
		}
		return -1;
	}

	public double getOutputBufferLifetimeInMillis() {
		if (this.isInChain()) {
			return 0;
		}
		if (this.outputBufferLifetimeStatistic.hasValues()) {
			return this.outputBufferLifetimeStatistic.getArithmeticMean();
		}
		return -1;
	}

	public double getRecordsPerBuffer() {
		if (this.recordsPerBufferStatistic.hasValues()) {
			return this.recordsPerBufferStatistic.getArithmeticMean();
		}
		return -1;
	}

	public double getRecordsPerSecond() {
		if (this.recordsPerSecondStatistic.hasValues()) {
			return this.recordsPerSecondStatistic.getArithmeticMean();
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

	public boolean isChannelLatencyFresherThan(long freshnessThreshold) {
		return this.latencyInMillisStatistic.getOldestValue().getTimestamp() >= freshnessThreshold;
	}

	public boolean isOutputBufferLifetimeFresherThan(long freshnessThreshold) {
		if (this.isInChain()) {
			return true;
		}
		return this.outputBufferLifetimeStatistic.getOldestValue()
				.getTimestamp() >= freshnessThreshold;
	}

	public void setIsInChain(boolean isInChain) {
		this.isInChain = isInChain;
	}

	public boolean isInChain() {
		return this.isInChain;
	}

	public boolean isActive() {
		return this.latencyInMillisStatistic.hasValues()
				&& (this.isInChain() || this.outputBufferLifetimeStatistic
						.hasValues());
	}

	public BufferSizeHistory getBufferSizeHistory() {
		return this.bufferSizeHistory;
	}

	public int getBufferSize() {
		return this.bufferSizeHistory.getLastEntry().getBufferSize();
	}

	public int getTargetOutputBufferLatency() {
		return targetOutputBufferLatency;
	}

	public void setTargetOutputBufferLatency(int targetOutputBufferLatency) {
		this.targetOutputBufferLatency = targetOutputBufferLatency;
	}
}
