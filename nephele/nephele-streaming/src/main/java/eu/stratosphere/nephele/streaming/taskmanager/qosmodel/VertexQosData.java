package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import java.lang.reflect.Array;

import eu.stratosphere.nephele.streaming.message.qosreport.VertexStatistics;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.sampling.Sample;
import eu.stratosphere.nephele.streaming.util.StreamPluginConfig;

/**
 * Instances of this class hold Qos data (currently only latency) of a
 * {@link QosVertex}. Vertex latency is only measured for those input/output
 * gate combinations that are covered by a Qos constraint.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class VertexQosData {

	private final QosVertex vertex;
	
	/**
	 * Sparse array indexed by inputGateIndex containing the gate's record
	 * consumption rate per second
	 */
	private QosStatistic[] igRecordsConsumedPerSec;

	/**
	 * Sparse array indexed by outputGateIndex containing the gate's record
	 * emission rate per second
	 */
	private QosStatistic[] ogRecordsEmittedPerSec;
	
	/**
	 * Sparse array indexed by (inputGateIndex, outputGateIndex) of the vertex.
	 * Sparseness means that some (inputGateIndex, outputGateIndex) of the array
	 * may be null.
	 */
	private QosStatistic[][] igOgVertexLatency;

	private QosStatistic[] igRecordInterArrivalTime;

	public VertexQosData(QosVertex vertex) {
		this.vertex = vertex;		
		this.igRecordsConsumedPerSec = new QosStatistic[1];
		this.ogRecordsEmittedPerSec = new QosStatistic[1];
		this.igOgVertexLatency = new QosStatistic[1][1];
		this.igRecordInterArrivalTime = new QosStatistic[1];
	}

	public QosVertex getVertex() {
		return this.vertex;
	}
	
	public double getLatencyInMillis(int inputGateIndex, int outputGateIndex) {
		if (igOgVertexLatency[inputGateIndex][outputGateIndex].hasValues()) {
			return igOgVertexLatency[inputGateIndex][outputGateIndex].getMean();
		}
		return -1;
	}

	public double getLatencyVarianceInMillis(int inputGateIndex, int outputGateIndex) {
		if (igOgVertexLatency[inputGateIndex][outputGateIndex].hasValues()) {
			return igOgVertexLatency[inputGateIndex][outputGateIndex].getVariance();
		}
		return -1;
	}

	public double getRecordsConsumedPerSec(int inputGateIndex) {
		if (igRecordsConsumedPerSec[inputGateIndex].hasValues()) {
			return igRecordsConsumedPerSec[inputGateIndex].getMean();
		}
		return -1;
	}

	public double getRecordsEmittedPerSec(int outputGateIndex) {
		if (ogRecordsEmittedPerSec[outputGateIndex].hasValues()) {
			return ogRecordsEmittedPerSec[outputGateIndex].getMean();
		}
		return -1;
	}

	public double getInterArrivalTimeInMillis(int inputGateIndex) {
		if (igRecordInterArrivalTime[inputGateIndex].hasValues()) {
			return igRecordInterArrivalTime[inputGateIndex].getMean();
		}
		return -1;
	}

	public double getInterArrivalTimeVarianceInMillis(int inputGateIndex) {
		if (igRecordInterArrivalTime[inputGateIndex].hasValues()) {
			return igRecordInterArrivalTime[inputGateIndex].getVariance();
		}
		return -1;
	}


	public void prepareForReportsOnGateCombination(int inputGateIndex,
			int outputGateIndex) {

		if (igOgVertexLatency.length <= inputGateIndex ||
				igOgVertexLatency[inputGateIndex] == null) {
			
			igOgVertexLatency = setInArray(QosStatistic[].class,
					igOgVertexLatency, 
					inputGateIndex, 
					new QosStatistic[outputGateIndex + 1]);
		}

		igOgVertexLatency[inputGateIndex] = setInArray(
				QosStatistic.class,
				igOgVertexLatency[inputGateIndex], outputGateIndex,
				new QosStatistic(StreamPluginConfig.computeQosStatisticWindowSize(), true));
		
		prepareForReportsOnInputGate(inputGateIndex);
		prepareForReportsOnOutputGate(outputGateIndex);
	}
	
	
	public void prepareForReportsOnInputGate(int inputGateIndex) {
		igRecordsConsumedPerSec = setInArray(QosStatistic.class,
				igRecordsConsumedPerSec, inputGateIndex,
				new QosStatistic(StreamPluginConfig.computeQosStatisticWindowSize()));
		
		
		igRecordInterArrivalTime = setInArray(QosStatistic.class,
				igRecordInterArrivalTime, inputGateIndex,
				new QosStatistic(StreamPluginConfig.computeQosStatisticWindowSize(), true));
	} 

	public void prepareForReportsOnOutputGate(int outputGateIndex) {
		ogRecordsEmittedPerSec = setInArray(QosStatistic.class,
				ogRecordsEmittedPerSec, outputGateIndex,
				new QosStatistic(StreamPluginConfig.computeQosStatisticWindowSize()));
	}
	
	private <T> T[] setInArray(Class<T> clazz, T[] array, int index, T value) {
		if (array.length <= index) {
			@SuppressWarnings("unchecked")
			T[] extendedArray = (T[]) Array.newInstance(clazz, index + 1);
			System.arraycopy(array, 0, extendedArray, 0,
					array.length);
			array = extendedArray;
		}

		if (array[index] == null) {
			array[index] = value;
		}
		
		return array;
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
     * @param measurement the vertex statistics
     */
	public void addVertexStatisticsMeasurement(int inputGateIndex, int outputGateIndex,
			long timestamp, VertexStatistics measurement) {

		if (inputGateIndex != -1 && outputGateIndex != -1) {

			Sample vertexLatency = measurement.getVertexLatencyMillis();
			QosStatistic stat = igOgVertexLatency[inputGateIndex][outputGateIndex];
			
			stat.addValue(new QosValue(vertexLatency.getMean(), vertexLatency.getVariance(), vertexLatency.getNoOfSamplePoints(), timestamp));
		}

		if (inputGateIndex != -1) {
			igRecordsConsumedPerSec[inputGateIndex]
					.addValue(new QosValue(measurement
							.getRecordsConsumedPerSec(), timestamp));

			Sample interarrivalTime = measurement.getInterArrivalTimeMillis();
			igRecordInterArrivalTime[inputGateIndex].addValue(new QosValue(
					interarrivalTime.getMean(), interarrivalTime.getVariance(),
					interarrivalTime.getNoOfSamplePoints(),
					timestamp));
		}

		if (outputGateIndex != -1) {
			ogRecordsEmittedPerSec[outputGateIndex]
					.addValue(new QosValue(measurement
							.getRecordsEmittedPerSec(), timestamp));

		}
	}
	
	public boolean hasNewerData(int inputGateIndex, int outputGateIndex, long thresholdTimestamp) {	
		if (inputGateIndex != -1) {
			return isInputGateConsumptionRateNewerThan(inputGateIndex, thresholdTimestamp);
		}

		if (outputGateIndex != -1) {
			return isOutputGateEmissionRateNewerThan(outputGateIndex, thresholdTimestamp);
		}

		return false;
	}
	
	public void dropOlderData(int inputGateIndex, int outputGateIndex,
			long thresholdTimestamp) {

		if (inputGateIndex != -1
				&& !isInputGateConsumptionRateNewerThan(inputGateIndex,
						thresholdTimestamp)) {

			igRecordsConsumedPerSec[inputGateIndex].clear();
		}

		if (outputGateIndex != -1
				&& !isOutputGateEmissionRateNewerThan(outputGateIndex,
						thresholdTimestamp)) {

			ogRecordsEmittedPerSec[outputGateIndex].clear();
		}

		if (inputGateIndex != -1
				&& outputGateIndex != -1
				&& !isVertexLatencyNewerThan(inputGateIndex, outputGateIndex,
						thresholdTimestamp)) {
			igOgVertexLatency[inputGateIndex][outputGateIndex].clear();
		}

	}
		
	private boolean isVertexLatencyNewerThan(int inputGateIndex,
			int outputGateIndex, long thresholdTimestamp) {

		if (!igOgVertexLatency[inputGateIndex][outputGateIndex].hasValues()) {
			return false;
		}

		return igOgVertexLatency[inputGateIndex][outputGateIndex]
				.getOldestValue().getTimestamp() >= thresholdTimestamp;
	}
	
	private boolean isInputGateConsumptionRateNewerThan(int inputGateIndex,
			long thresholdTimestamp) {

		if (!igRecordsConsumedPerSec[inputGateIndex].hasValues()) {
			return false;
		}

		return igRecordsConsumedPerSec[inputGateIndex].getOldestValue()
				.getTimestamp() >= thresholdTimestamp;
	}
	
	private boolean isOutputGateEmissionRateNewerThan(int outputGateIndex,
			long thresholdTimestamp) {

		if (!ogRecordsEmittedPerSec[outputGateIndex].hasValues()) {
			return false;
		}

		return ogRecordsEmittedPerSec[outputGateIndex].getOldestValue()
				.getTimestamp() >= thresholdTimestamp;
	}	
}
