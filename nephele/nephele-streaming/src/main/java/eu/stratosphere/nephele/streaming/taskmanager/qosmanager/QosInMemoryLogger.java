package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.HistoryEntry;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.ValueHistory;
import eu.stratosphere.nephele.streaming.util.StreamPluginConfig;

public class QosInMemoryLogger extends AbstractQosLogger {

	private final ValueHistory<QosConstraintSummary> history;
	private final JSONArray latencyTypes;
	private final JSONArray latencyLabels;
	private final JSONArray emitConsumeDescriptions;

	public QosInMemoryLogger(ExecutionGraph execGraph, JobGraphLatencyConstraint constraint, long loggingInterval) {
		super(loggingInterval);
		this.latencyTypes = getLatencyTypesHeader(constraint.getSequence());
		this.latencyLabels = getLatencyLabelsHeader(constraint.getSequence());
		this.emitConsumeDescriptions = getEmitConsumeEdgeDescriptions(execGraph, constraint.getSequence());
		this.history = new ValueHistory<QosConstraintSummary>(StreamPluginConfig.getNoOfInMemoryLogEntries());
	}

	public int getMaxEntriesCount() {
		return this.history.getMaxNumberOfEntries();
	}

	private JSONArray getLatencyLabelsHeader(JobGraphSequence jobGraphSequence) {
		JSONArray header = new JSONArray();

		for (SequenceElement sequenceElement : jobGraphSequence) {
			if (sequenceElement.isVertex()) {
				header.put(sequenceElement.getName());
			} else {
				header.put(0);
				header.put(0);
			}
		}

		return header;
	}

	private JSONArray getEmitConsumeEdgeDescriptions(ExecutionGraph execGraph, JobGraphSequence jobGraphSequence) {
		JSONArray descriptions = new JSONArray();

		for (SequenceElement sequenceElement : jobGraphSequence) {
			if (sequenceElement.isEdge()) {
				JSONArray description = new JSONArray();
				description.put(execGraph.getExecutionGroupVertex(sequenceElement.getSourceVertexID()).getName());
				description.put(execGraph.getExecutionGroupVertex(sequenceElement.getTargetVertexID()).getName());
				descriptions.put(description);
			}
		}

		return descriptions;
	}

	private JSONArray getLatencyTypesHeader(JobGraphSequence jobGraphSequence) {
		JSONArray header = new JSONArray();

		for (SequenceElement sequenceElement : jobGraphSequence) {
			if (sequenceElement.isVertex()) {
				header.put("vertex");
			} else {
				header.put("edgeObl");
				header.put("edge");
			}
		}

		return header;
	}

	public void logSummary(QosConstraintSummary summary) throws JSONException {
		this.history.addToHistory(getLogTimestamp(), summary);
	}

	public JSONObject toJson(JSONObject json) throws JSONException {
		return toJson(json, this.history.getEntries(), true);
	}

	public JSONObject toJson(JSONObject json, long minTimestamp) throws JSONException {
		return toJson(json, this.history.getLastEntries(minTimestamp), false);
	}

	private JSONObject toJson(JSONObject result, HistoryEntry<QosConstraintSummary> entries[],
			boolean withLabels) throws JSONException {

		JSONArray latencyEntries = new JSONArray();
		JSONArray emitConsume = new JSONArray();

		for (int e = 0; e < entries.length; e++) {
			long timestamp = entries[e].getTimestamp();
			QosConstraintSummary summary = entries[e].getValue();

			JSONObject latencyEntry = new JSONObject();
			latencyEntry.put("ts", timestamp);
			latencyEntry.put("min", summary.getViolationReport().getMinSequenceLatency());
			latencyEntry.put("max", summary.getViolationReport().getMaxSequenceLatency());
			JSONArray latencyValues = new JSONArray();
			latencyEntry.put("values", latencyValues);

			int edgeIndex = 0;

			boolean nextIsVertex = summary.doesSequenceStartWithVertex();
			for (int i = 0; i < summary.getSequenceLength(); i++) {
				if (nextIsVertex) {
					QosGroupVertexSummary vertexSum = summary.getGroupVertexSummary(i);
					// vertex
					latencyValues.put(vertexSum.getMeanVertexLatency());

				} else {
					QosGroupEdgeSummary edgeSum = summary.getGroupEdgeSummary(i);
					
					// edge
					latencyValues.put(edgeSum.getOutputBufferLatencyMean());
					latencyValues.put(edgeSum.getTransportLatencyMean());

					JSONObject ecEntry = new JSONObject();
					ecEntry.put("ts", timestamp);
					ecEntry.put("avgEmitRate", edgeSum.getMeanEmissionRate());
					ecEntry.put("avgConsumeRate", edgeSum.getMeanConsumptionRate());
					ecEntry.put("emittingVertexDop", edgeSum.getActiveEmitterVertices());
					ecEntry.put("consumingVertexDop", edgeSum.getActiveConsumerVertices());
					ecEntry.put("totalEmitRate", edgeSum.getMeanEmissionRate() * edgeSum.getActiveEmitterVertices());
					ecEntry.put("totalConsumeRate", edgeSum.getMeanConsumptionRate() * edgeSum.getActiveConsumerVertices());

					if (emitConsume.length() == edgeIndex)
						emitConsume.put(new JSONArray());

					emitConsume.getJSONArray(edgeIndex).put(ecEntry);

					edgeIndex++;
				}
				nextIsVertex = !nextIsVertex;
			}

			latencyEntries.put(latencyEntry);
		}

		JSONObject latency = new JSONObject();
		if (withLabels) {
			latency.put("types", this.latencyTypes);
			latency.put("labels", this.latencyLabels);
		}
		latency.put("rows", latencyEntries);
		result.put("latencies", latency);

		result.put("emitConsume", emitConsume);
		if (withLabels) {
			result.put("emitConsumeDescriptions", this.emitConsumeDescriptions);
		}

		return result;
	}
}
