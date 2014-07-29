package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import eu.stratosphere.nephele.streaming.message.qosreport.VertexStatistics;

/**
 * Instances of this class hold Qos data (currently only latency) of a
 * {@link QosVertex}. Vertex latency is only measured for those input/output
 * gate combinations that are covered by a Qos constraint.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class VertexQosData {

	private QosVertex vertex;

	public class GateCombinationStatistics {
		QosStatistic latency = new QosStatistic(
				DEFAULT_NO_OF_STATISTICS_ENTRIES);
		QosStatistic recordsConsumedPerSec = new QosStatistic(
				DEFAULT_NO_OF_STATISTICS_ENTRIES);
		QosStatistic recordsEmittedPerSec = new QosStatistic(
				DEFAULT_NO_OF_STATISTICS_ENTRIES);

		public double getLatencyInMillis() {
			if (latency.hasValues()) {
				return latency.getArithmeticMean();
			}
			return -1;
		}

		public double getRecordsConsumedPerSec() {
			if (recordsConsumedPerSec.hasValues()) {
				return recordsConsumedPerSec.getArithmeticMean();
			}
			return -1;
		}

		public double getRecordsEmittedPerSec() {
			if (recordsEmittedPerSec.hasValues()) {
				return recordsEmittedPerSec.getArithmeticMean();
			}
			return -1;
		}
	}
	
	/**
	 * Sparse array indexed by (inputGateIndex, outputGateIndex) of the vertex.
	 * Sparseness means that some (inputGateIndex, outputGateIndex) of the array
	 * may be null.
	 */
	private GateCombinationStatistics[][] gateCombinationStatistics;

	private final static int DEFAULT_NO_OF_STATISTICS_ENTRIES = 4;

	public VertexQosData(QosVertex vertex) {
		this.vertex = vertex;
		this.gateCombinationStatistics = new GateCombinationStatistics[1][1];
	}

	public QosVertex getVertex() {
		return this.vertex;
	}

	
	public GateCombinationStatistics getGateCombinationStatistics(int inputGateIndex, int outputGateIndex) {
		return this.gateCombinationStatistics[inputGateIndex][outputGateIndex];
	}

	public boolean isActive(int inputGateIndex, int outputGateIndex) {
		return this.gateCombinationStatistics[inputGateIndex][outputGateIndex].latency.hasValues();
	}

	public void prepareForReportsOnGateCombination(int inputGateIndex,
			int outputGateIndex) {

		if (this.gateCombinationStatistics.length <= inputGateIndex) {
			GateCombinationStatistics[][] newArray = new GateCombinationStatistics[inputGateIndex + 1][];
			System.arraycopy(this.gateCombinationStatistics, 0, newArray, 0,
					this.gateCombinationStatistics.length);
            this.gateCombinationStatistics = newArray;
		}

		if (this.gateCombinationStatistics[inputGateIndex] == null) {
			this.gateCombinationStatistics[inputGateIndex] = new GateCombinationStatistics[outputGateIndex + 1];
		}

		if (this.gateCombinationStatistics[inputGateIndex].length <= outputGateIndex) {
			GateCombinationStatistics[] newArray = new GateCombinationStatistics[outputGateIndex + 1];
			System.arraycopy(this.gateCombinationStatistics[inputGateIndex], 0, newArray,
					0, this.gateCombinationStatistics[inputGateIndex].length);
            this.gateCombinationStatistics[inputGateIndex] = newArray;
		}

		if (this.gateCombinationStatistics[inputGateIndex][outputGateIndex] == null) {
			this.gateCombinationStatistics[inputGateIndex][outputGateIndex] = new GateCombinationStatistics();
		}
	}

    /**
     * Adds a vertex QoS statistics measurement.
     *
     * Please note: Before submitting data to this method
     * {#prepareForReportsOnGateCombination} has to be called to prepare
     * internal data structures.
     *
     * @param inputGateIndex the input gate index
     * @param outputGateIndex the output gate index
     * @param timestamp the current timestamp as a long
     * @param latencyInMillis the current latency in milliseconds as a double
     */
	public void addVertexStatisticsMeasurement(int inputGateIndex, int outputGateIndex,
			long timestamp, VertexStatistics measurement) {

		GateCombinationStatistics stats = this.gateCombinationStatistics[inputGateIndex][outputGateIndex];
		
		stats.latency.addValue(new QosValue(measurement.getVertexLatency(), timestamp));
		stats.recordsConsumedPerSec.addValue(new QosValue(measurement.getRecordsConsumedPerSec(), timestamp));
		stats.recordsEmittedPerSec.addValue(new QosValue(measurement.getRecordsEmittedPerSec(), timestamp));
	}
}
