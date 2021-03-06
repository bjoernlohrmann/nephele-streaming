package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class QosGroupVertexSummary implements QosGroupElementSummary {

	private int activeVertices = 0;

	private double meanVertexLatency = 0;

	private double meanVertexLatencyCV = 0;

	public double getMeanVertexLatency() {
		return meanVertexLatency;
	}

	public void setMeanVertexLatency(double meanVertexLatency) {
		this.meanVertexLatency = meanVertexLatency;
	}

	public double getMeanVertexLatencyCV() {
		return meanVertexLatencyCV;
	}

	public void setMeanVertexLatencyCV(double meanVertexLatencyCV) {
		this.meanVertexLatencyCV = meanVertexLatencyCV;
	}

	public int getActiveVertices() {
		return activeVertices;
	}

	public void setActiveVertices(int activeVertices) {
		this.activeVertices = activeVertices;
	}

	@Override
	public boolean isVertex() {
		return true;
	}

	@Override
	public boolean isEdge() {
		return false;
	}

	@Override
	public void merge(List<QosGroupElementSummary> elemSummaries) {
		for (QosGroupElementSummary elemSum : elemSummaries) {
			QosGroupVertexSummary toMerge = (QosGroupVertexSummary) elemSum;
			
			activeVertices += toMerge.activeVertices;
			
			meanVertexLatency += toMerge.activeVertices
					* toMerge.meanVertexLatency;
			
			meanVertexLatencyCV += toMerge.activeVertices
					* toMerge.meanVertexLatencyCV;
		}

		if (activeVertices > 0) {
			meanVertexLatency /= activeVertices;
			meanVertexLatencyCV /= activeVertices;
		}
	}

	@Override
	public boolean hasData() {
		return activeVertices > 0;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(activeVertices);
		out.writeDouble(meanVertexLatency);
		out.writeDouble(meanVertexLatencyCV);
	}

	@Override
	public void read(DataInput in) throws IOException {
		activeVertices = in.readInt();
		meanVertexLatency = in.readDouble();
		meanVertexLatencyCV = in.readDouble();
	}
}
