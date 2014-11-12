package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.plugins.PluginManager;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.HistoryEntry;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.QosConstraintSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.ValueHistory;

public class QosInMemoryLogger extends AbstractQosLogger {
	private static final String LOG_ENTRIES_KEY = PluginManager
			.prefixWithPluginNamespace("streaming.qosmanager.logging.in_memory_entries");

	private static final int DEFAULT_ENTRIES_COUNT = 15 * 60 / 10; // = 15min @ 10s logging interval

	private final ValueHistory<QosConstraintSummary> history;
	private final JSONArray latencyTypes;
	private final JSONArray latencyLabels;
	private final JSONArray emitConsumeDescriptions;

	public QosInMemoryLogger(ExecutionGraph execGraph, JobGraphLatencyConstraint constraint, long loggingInterval) {
		super(loggingInterval);
		this.latencyTypes = getLatencyTypesHeader(constraint.getSequence());
		this.latencyLabels = getLatencyLabelsHeader(constraint.getSequence());
		this.emitConsumeDescriptions = getEmitConsumeEdgeDescriptions(execGraph, constraint.getSequence());
		int noOfHistoryEntries = GlobalConfiguration.getInteger(LOG_ENTRIES_KEY, DEFAULT_ENTRIES_COUNT);
		this.history = new ValueHistory<QosConstraintSummary>(noOfHistoryEntries);
	}

	public int getMaxEntriesCount() {
		return this.history.getMaxNumberOfEntries();
	}

	private JSONArray getLatencyLabelsHeader(JobGraphSequence jobGraphSequence) {
		JSONArray header = new JSONArray();

		for (SequenceElement<JobVertexID> sequenceElement : jobGraphSequence) {
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

		for (SequenceElement<JobVertexID> sequenceElement : jobGraphSequence) {
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

		for (SequenceElement<JobVertexID> sequenceElement : jobGraphSequence) {
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
			latencyEntry.put("min", summary.getMinSequenceLatency());
			latencyEntry.put("max", summary.getMaxSequenceLatency());
			JSONArray latencyValues = new JSONArray();
			latencyEntry.put("values", latencyValues);

			double[][] memberStats = summary.getAggregatedMemberStatistics();
			int[] taskDop = summary.getTaskDop();
			int edgeIndex = 0;

			for (int i = 0; i < memberStats.length; i++) {
				if (memberStats[i].length == 1) {
					// vertex
					latencyValues.put(memberStats[i][0]);

				} else {
					// edge
					latencyValues.put(memberStats[i][0]);
					latencyValues.put(memberStats[i][1]);

					JSONObject ecEntry = new JSONObject();
					ecEntry.put("ts", timestamp);
					ecEntry.put("avgEmitRate", memberStats[i][2]);
					ecEntry.put("avgConsumeRate", memberStats[i][3]);
					ecEntry.put("emittingVertexDop", taskDop[edgeIndex]);
					ecEntry.put("consumingVertexDop", taskDop[edgeIndex + 1]);
					ecEntry.put("totalEmitRate", memberStats[i][2] * taskDop[edgeIndex]);
					ecEntry.put("totalConsumeRate", memberStats[i][3] * taskDop[edgeIndex + 1]);

					if (emitConsume.length() == edgeIndex)
						emitConsume.put(new JSONArray());

					emitConsume.getJSONArray(edgeIndex).put(ecEntry);

					edgeIndex++;
				}
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
