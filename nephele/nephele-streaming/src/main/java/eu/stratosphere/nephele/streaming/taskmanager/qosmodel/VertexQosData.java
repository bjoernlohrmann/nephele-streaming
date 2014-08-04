package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import java.lang.reflect.Array;

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
	
	
	/**
	 * Sparse array indexed by inputGateIndex containing the gate's record
	 * consumption rate per second
	 */
	private QosStatistic[] inputGateRecordsConsumedPerSec;

	/**
	 * Sparse array indexed by outputGateIndex containing the gate's record
	 * emission rate per second
	 */
	private QosStatistic[] outputGateRecordsEmittedPerSec;
	
	/**
	 * Sparse array indexed by (inputGateIndex, outputGateIndex) of the vertex.
	 * Sparseness means that some (inputGateIndex, outputGateIndex) of the array
	 * may be null.
	 */
	private QosStatistic[][] inputOutputGateLatency;
	

	private final static int DEFAULT_NO_OF_STATISTICS_ENTRIES = 4;

	public VertexQosData(QosVertex vertex) {
		this.vertex = vertex;
		this.inputOutputGateLatency = new QosStatistic[1][1];
		this.inputGateRecordsConsumedPerSec = new QosStatistic[1];
		this.outputGateRecordsEmittedPerSec = new QosStatistic[1];
	}

	public QosVertex getVertex() {
		return this.vertex;
	}
	
	public double getLatencyInMillis(int inputGateIndex, int outputGateIndex) {
		if (inputOutputGateLatency[inputGateIndex][outputGateIndex].hasValues()) {
			return inputOutputGateLatency[inputGateIndex][outputGateIndex].getArithmeticMean();
		}
		return -1;
	}

	public double getRecordsConsumedPerSec(int inputGateIndex) {
		if (inputGateRecordsConsumedPerSec[inputGateIndex].hasValues()) {
			return inputGateRecordsConsumedPerSec[inputGateIndex].getArithmeticMean();
		}
		return -1;
	}

	public double getRecordsEmittedPerSec(int outputGateIndex) {
		if (outputGateRecordsEmittedPerSec[outputGateIndex].hasValues()) {
			return outputGateRecordsEmittedPerSec[outputGateIndex].getArithmeticMean();
		}
		return -1;
	}

	public void prepareForReportsOnGateCombination(int inputGateIndex,
			int outputGateIndex) {

		if (inputOutputGateLatency.length <= inputGateIndex ||
				inputOutputGateLatency[inputGateIndex] == null) {
			
			inputOutputGateLatency = setInArray(QosStatistic[].class,
					inputOutputGateLatency, 
					inputGateIndex, 
					new QosStatistic[outputGateIndex + 1]);
		}

		inputOutputGateLatency[inputGateIndex] = setInArray(
				QosStatistic.class,
				inputOutputGateLatency[inputGateIndex], outputGateIndex,
				new QosStatistic(DEFAULT_NO_OF_STATISTICS_ENTRIES));
		
		prepareForReportsOnInputGate(inputGateIndex);
		prepareForReportsOnOutputGate(outputGateIndex);
	}
	
	
	public void prepareForReportsOnInputGate(int inputGateIndex) {
		inputGateRecordsConsumedPerSec = setInArray(QosStatistic.class,
				inputGateRecordsConsumedPerSec, inputGateIndex,
				new QosStatistic(DEFAULT_NO_OF_STATISTICS_ENTRIES));

	} 

	public void prepareForReportsOnOutputGate(int outputGateIndex) {
		outputGateRecordsEmittedPerSec = setInArray(QosStatistic.class,
				outputGateRecordsEmittedPerSec, outputGateIndex,
				new QosStatistic(DEFAULT_NO_OF_STATISTICS_ENTRIES));
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
     * @param latencyInMillis the current latency in milliseconds as a double
     */
	public void addVertexStatisticsMeasurement(int inputGateIndex, int outputGateIndex,
			long timestamp, VertexStatistics measurement) {

		if (inputGateIndex != -1 && outputGateIndex != -1) {

			QosStatistic stat = inputOutputGateLatency[inputGateIndex][outputGateIndex];

			stat.addValue(new QosValue(measurement.getVertexLatency(),
					timestamp));

		}

		if (inputGateIndex != -1) {
			inputGateRecordsConsumedPerSec[inputGateIndex]
					.addValue(new QosValue(measurement
							.getRecordsConsumedPerSec(), timestamp));

		}

		if (outputGateIndex != -1) {
			outputGateRecordsEmittedPerSec[outputGateIndex]
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

			inputGateRecordsConsumedPerSec[inputGateIndex].clear();
		}

		if (outputGateIndex != -1
				&& !isOutputGateEmissionRateNewerThan(outputGateIndex,
						thresholdTimestamp)) {

			outputGateRecordsEmittedPerSec[outputGateIndex].clear();
		}

		if (inputGateIndex != -1
				&& outputGateIndex != -1
				&& !isVertexLatencyNewerThan(inputGateIndex, outputGateIndex,
						thresholdTimestamp)) {
			inputOutputGateLatency[inputGateIndex][outputGateIndex].clear();
		}

	}
		
	private boolean isVertexLatencyNewerThan(int inputGateIndex,
			int outputGateIndex, long thresholdTimestamp) {

		if (!inputOutputGateLatency[inputGateIndex][outputGateIndex].hasValues()) {
			return false;
		}

		return inputOutputGateLatency[inputGateIndex][outputGateIndex]
				.getOldestValue().getTimestamp() >= thresholdTimestamp;
	}
	
	private boolean isInputGateConsumptionRateNewerThan(int inputGateIndex,
			long thresholdTimestamp) {

		if (!inputGateRecordsConsumedPerSec[inputGateIndex].hasValues()) {
			return false;
		}

		return inputGateRecordsConsumedPerSec[inputGateIndex].getOldestValue()
				.getTimestamp() >= thresholdTimestamp;
	}
	
	private boolean isOutputGateEmissionRateNewerThan(int outputGateIndex,
			long thresholdTimestamp) {

		if (!outputGateRecordsEmittedPerSec[outputGateIndex].hasValues()) {
			return false;
		}

		return outputGateRecordsEmittedPerSec[outputGateIndex].getOldestValue()
				.getTimestamp() >= thresholdTimestamp;
	}	
}
