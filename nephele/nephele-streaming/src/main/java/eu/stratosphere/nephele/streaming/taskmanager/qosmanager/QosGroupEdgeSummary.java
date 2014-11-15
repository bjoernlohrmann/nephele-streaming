package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;

public class QosGroupEdgeSummary implements QosGroupElementSummary {

	/**
	 * The number of active member group edges.
	 */
	private int activeEdges;

	/**
	 * The mean output buffer latency of the group edge's {@link QosEdge}
	 * members.
	 */
	private double outputBufferLatencyMean;

	/**
	 * The average remaining transport latency (i.e. channel latency without
	 * output buffer latency) of the group edge's {@link QosEdge} members.
	 */
	private double transportLatencyMean;

	/**
	 * The number of source member vertices actively writing into the group
	 * edge's {@link QosEdge} member edges.
	 */
	private int activeEmitterVertices;

	/**
	 * The number of target member vertices actively reading from the group
	 * edge's {@link QosEdge} member edges. This is actually an int but
	 * represented as a double.
	 */
	private int activeConsumerVertices;

	/**
	 * The mean number of records per second that the source member vertices
	 * write into the group edge's {@link QosEdge} member edges. Example: With
	 * one source member vertex that emits 20 rec/s and one that emits 10 rec/s,
	 * this variable will be 15.
	 */
	private double meanEmissionRate;

	/**
	 * The mean number of records per second that the target member vertices
	 * read from the group edge's {@link QosEdge} member edges. Example: With
	 * one target member vertex consuming 20 rec/s and one consuming 10 rec/s,
	 * this variable will be 15.
	 */
	private double meanConsumptionRate;

	/**
	 * Each active target member vertex reads records from the group edge's
	 * {@link QosEdge} member edges. For such a member vertex we measure the
	 * mean time it takes the vertex to process a record. This is also referred
	 * to as "vertex latency". Since vertex latency is not constant we compute
	 * its mean and variance for each member. This variable then holds the mean
	 * of the mean vertex latencies of all active target member vertices.
	 * 
	 * <p>
	 * In general the following equation holds: meanConsumerVertexLatency <=
	 * 1/meanConsumptionRate, because consumption is throttled when no input is
	 * available. If input is always available, then meanConsumerVertexLatency
	 * =~ 1/meanConsumptionRate.
	 * </p>
	 */
	private double meanConsumerVertexLatency;

	/**
	 * See {@link #meanConsumerVertexLatency}. This is the mean of the vertex
	 * latency variances.
	 */
	private double meanConsumerVertexLatencyVariance;

	private double meanConsumerVertexInterarrivalTime;

	private double meanConsumerVertexInterarrivalTimeVariance;

	public QosGroupEdgeSummary() {
	}

	public int getActiveEdges() {
		return activeEdges;
	}

	public void setActiveEdges(int activeEdges) {
		this.activeEdges = activeEdges;
	}

	public double getOutputBufferLatencyMean() {
		return outputBufferLatencyMean;
	}

	public void setOutputBufferLatencyMean(double outputBufferLatencyMean) {
		this.outputBufferLatencyMean = outputBufferLatencyMean;
	}

	public double getTransportLatencyMean() {
		return transportLatencyMean;
	}

	public void setTransportLatencyMean(double transportLatencyMean) {
		this.transportLatencyMean = transportLatencyMean;
	}

	public int getActiveEmitterVertices() {
		return activeEmitterVertices;
	}

	public void setActiveEmitterVertices(int activeEmitterVertices) {
		this.activeEmitterVertices = activeEmitterVertices;
	}

	public int getActiveConsumerVertices() {
		return activeConsumerVertices;
	}

	public void setActiveConsumerVertices(int activeConsumerVertices) {
		this.activeConsumerVertices = activeConsumerVertices;
	}

	public double getMeanEmissionRate() {
		return meanEmissionRate;
	}

	public void setMeanEmissionRate(double meanEmissionRate) {
		this.meanEmissionRate = meanEmissionRate;
	}

	public double getMeanConsumptionRate() {
		return meanConsumptionRate;
	}

	public void setMeanConsumptionRate(double meanConsumptionRate) {
		this.meanConsumptionRate = meanConsumptionRate;
	}

	public double getMeanConsumerVertexLatency() {
		return meanConsumerVertexLatency;
	}

	public void setMeanConsumerVertexLatency(double meanConsumerVertexLatency) {
		this.meanConsumerVertexLatency = meanConsumerVertexLatency;
	}

	public double getMeanConsumerVertexLatencyVariance() {
		return meanConsumerVertexLatencyVariance;
	}

	public void setMeanConsumerVertexLatencyVariance(
			double meanConsumerVertexLatencyVariance) {
		this.meanConsumerVertexLatencyVariance = meanConsumerVertexLatencyVariance;
	}

	public double getMeanConsumerVertexInterarrivalTime() {
		return meanConsumerVertexInterarrivalTime;
	}

	public void setMeanConsumerVertexInterarrivalTime(
			double meanConsumerVertexInterarrivalTime) {
		this.meanConsumerVertexInterarrivalTime = meanConsumerVertexInterarrivalTime;
	}

	public double getMeanConsumerVertexInterarrivalTimeVariance() {
		return meanConsumerVertexInterarrivalTimeVariance;
	}

	public void setMeanConsumerVertexInterarrivalTimeVariance(
			double meanConsumerVertexInterarrivalTimeVariance) {
		this.meanConsumerVertexInterarrivalTimeVariance = meanConsumerVertexInterarrivalTimeVariance;
	}

	@Override
	public boolean isVertex() {
		return false;
	}

	@Override
	public boolean isEdge() {
		return true;
	}

	@Override
	public boolean hasData() {
		return (activeEdges == -1 || activeEdges > 0) && activeConsumerVertices > 0
				&& activeEmitterVertices > 0;
	}

	@Override
	public void merge(List<QosGroupElementSummary> elemSummaries) {
		for (QosGroupElementSummary elemSum : elemSummaries) {
			QosGroupEdgeSummary toMerge = (QosGroupEdgeSummary) elemSum;

			activeEdges += toMerge.activeEdges;

			outputBufferLatencyMean += toMerge.activeEdges
					* toMerge.outputBufferLatencyMean;

			transportLatencyMean += toMerge.activeEdges
					* toMerge.transportLatencyMean;

			activeEmitterVertices += toMerge.activeEmitterVertices;

			meanEmissionRate += toMerge.activeEmitterVertices
					* toMerge.meanEmissionRate;

			activeConsumerVertices += toMerge.activeConsumerVertices;

			meanConsumptionRate += toMerge.activeConsumerVertices
					* toMerge.meanConsumptionRate;

			meanConsumerVertexLatency += toMerge.activeConsumerVertices
					* toMerge.meanConsumerVertexLatency;

			meanConsumerVertexLatencyVariance += toMerge.activeConsumerVertices
					* toMerge.meanConsumerVertexLatencyVariance;

			meanConsumerVertexInterarrivalTime += toMerge.activeConsumerVertices
					* toMerge.meanConsumerVertexInterarrivalTime;

			meanConsumerVertexInterarrivalTimeVariance += toMerge.activeConsumerVertices
					* toMerge.meanConsumerVertexInterarrivalTimeVariance;
		}

		if (hasData()) {
			outputBufferLatencyMean /= activeEdges;
			transportLatencyMean /= activeEdges;

			meanEmissionRate /= activeEmitterVertices;

			meanConsumptionRate /= activeConsumerVertices;
			meanConsumerVertexLatency /= activeConsumerVertices;
			meanConsumerVertexLatencyVariance /= activeConsumerVertices;
			meanConsumerVertexInterarrivalTime /= activeConsumerVertices;
			meanConsumerVertexInterarrivalTimeVariance /= activeConsumerVertices;
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(activeEdges);
		out.writeDouble(outputBufferLatencyMean);
		out.writeDouble(transportLatencyMean);
		
		out.writeInt(activeEmitterVertices);
		out.writeDouble(meanEmissionRate);

		out.writeInt(activeConsumerVertices);
		out.writeDouble(meanConsumptionRate);
		out.writeDouble(meanConsumerVertexLatency);
		out.writeDouble(meanConsumerVertexLatencyVariance);
		out.writeDouble(meanConsumerVertexInterarrivalTime);
		out.writeDouble(meanConsumerVertexInterarrivalTimeVariance);

	}

	@Override
	public void read(DataInput in) throws IOException {
		activeEdges = in.readInt();
		outputBufferLatencyMean = in.readDouble();
		transportLatencyMean = in.readDouble();
		
		activeEmitterVertices = in.readInt();
		meanEmissionRate = in.readDouble();

		activeConsumerVertices = in.readInt();
		meanConsumptionRate = in.readDouble();
		meanConsumerVertexLatency = in.readDouble();
		meanConsumerVertexLatencyVariance = in.readDouble();
		meanConsumerVertexInterarrivalTime = in.readDouble();
		meanConsumerVertexInterarrivalTimeVariance = in.readDouble();
	}
}
