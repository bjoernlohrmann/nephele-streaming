package eu.stratosphere.nephele.streaming.jobmanager.autoscaling;

import java.io.IOException;
import java.util.Map;

import org.codehaus.jettison.json.JSONException;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosUtils;

public abstract class AbstractCpuLoadLogger {
	protected final long loggingInterval;
	protected final JobVertexID groupVertices[];

	public AbstractCpuLoadLogger(ExecutionGraph execGraph, JobGraphLatencyConstraint constraint, long loggingInterval) {
		this.loggingInterval = loggingInterval;
		this.groupVertices = constraint.getSequence().getVerticesForSequenceOrdered(true).toArray(new JobVertexID[0]);
	}

	public abstract void logCpuLoads(Map<JobVertexID, GroupVertexCpuLoadSummary> loadSummaries) throws JSONException, IOException;

	protected long getLogTimestamp() {
		return QosUtils.alignToInterval(System.currentTimeMillis(), this.loggingInterval);
	}

	public void close() throws IOException {
	};
}
