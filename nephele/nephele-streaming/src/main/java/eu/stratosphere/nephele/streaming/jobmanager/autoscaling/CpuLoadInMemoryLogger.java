package eu.stratosphere.nephele.streaming.jobmanager.autoscaling;

import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.plugins.PluginManager;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.HistoryEntry;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.ValueHistory;

public class CpuLoadInMemoryLogger extends AbstractCpuLoadLogger {
	private final ValueHistory<JSONObject> history;
	private final JSONArray header;

	private static final String LOG_ENTRIES_KEY = PluginManager.prefixWithPluginNamespace("streaming.qosmanager.logging.in_memory_entries");

	private static final int DEFAULT_ENTRY_COUNT = 15 * 60 / 10; // = 15min @ 10s logging interval

	public CpuLoadInMemoryLogger(ExecutionGraph execGraph, JobGraphLatencyConstraint constraint, long loggingInterval) throws JSONException {
		super(execGraph, constraint, loggingInterval);

		this.header = getHeader(execGraph);

		int noOfHistoryEntries = GlobalConfiguration.getInteger(LOG_ENTRIES_KEY, DEFAULT_ENTRY_COUNT);
		this.history = new ValueHistory<JSONObject>(noOfHistoryEntries);
	}

	private JSONArray getHeader(ExecutionGraph execGraph) throws JSONException {
		JSONArray header = new JSONArray();

		for (JobVertexID id : this.groupVertices) {
			JSONObject vertex = new JSONObject();
			vertex.put("id", id);
			vertex.put("name", execGraph.getExecutionGroupVertex(id).getName());
		 	header.put(vertex);
		}

		return header;
	}

	@Override
	public void logCpuLoads(Map<JobVertexID, GroupVertexCpuLoadSummary> loadSummaries) throws JSONException {
		long timestamp = getLogTimestamp();
		JSONObject entry = new JSONObject();
		entry.put("ts", timestamp);
		JSONArray values = new JSONArray();

		for (JobVertexID id : this.groupVertices) {
			JSONArray vertexValues = new JSONArray();
			GroupVertexCpuLoadSummary cpuLoad = loadSummaries.get(id);
			vertexValues.put(cpuLoad.getUnknowns());
			vertexValues.put(cpuLoad.getLows());
			vertexValues.put(cpuLoad.getMediums());
			vertexValues.put(cpuLoad.getHighs());
			values.put(vertexValues);
		}

		entry.put("values", values);

		this.history.addToHistory(timestamp, entry);
	}

	public JSONObject toJson(JSONObject json) throws JSONException {
		return toJson(json, this.history.getEntries());
	}

	public JSONObject toJson(JSONObject json, long minTimestamp) throws JSONException {
		return toJson(json, this.history.getLastEntries(minTimestamp));
	}

	private JSONObject toJson(JSONObject result, HistoryEntry<JSONObject> entries[]) throws JSONException {
		JSONArray values = new JSONArray();

		for (HistoryEntry<JSONObject> entry : entries) {
			values.put(entry.getValue());
		}

		JSONObject cpuLoads = new JSONObject();
		cpuLoads.put("header", this.header);
		cpuLoads.put("values", values);
		result.put("cpuLoads", cpuLoads);

		return result;
	}
}
