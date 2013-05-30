package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import eu.stratosphere.nephele.streaming.taskmanager.StreamTaskManagerPlugin;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGroupVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.ProfilingSequence;

public class ProfilingLogger {

	/**
	 * Provides access to the configuration entry which defines the log file
	 * location.
	 */
	private static final String PROFILING_LOGFILE_KEY = "streaming.qosmanager.logging.profilingfile";

	private static final String DEFAULT_LOGFILE = "/tmp/profiling_"
			+ System.getProperty("user.name") + ".txt";

	private BufferedWriter writer;

	private boolean headersWritten;

	private long timeOfNextLogging;

	private long loggingInterval;

	public ProfilingLogger(long loggingInterval) throws IOException {

		String logFile = StreamTaskManagerPlugin.getPluginConfiguration()
				.getString(PROFILING_LOGFILE_KEY, DEFAULT_LOGFILE);

		this.writer = new BufferedWriter(new FileWriter(logFile));

		this.loggingInterval = loggingInterval;
		this.headersWritten = false;
	}

	public boolean isLoggingNecessary(long now) {
		return now >= this.timeOfNextLogging;
	}

	public void logLatencies(ProfilingSequenceSummary summary)
			throws IOException {
		if (!this.headersWritten) {
			this.writeHeaders(summary);
		}

		StringBuilder builder = new StringBuilder();
		builder.append(this.getLogTimestamp());
		builder.append(';');
		builder.append(summary.getNoOfActiveSubsequences());
		builder.append(';');
		builder.append(summary.getNoOfInactiveSubsequences());
		builder.append(';');
		builder.append(this.formatDouble(summary.getAvgSubsequenceLatency()));
		builder.append(';');
		builder.append(this.formatDouble(summary.getMinSubsequenceLatency()));
		builder.append(';');
		builder.append(this.formatDouble(summary.getMaxSubsequenceLatency()));

		for (double avgElementLatency : summary
				.getAvgSequenceElementLatencies()) {
			builder.append(';');
			builder.append(this.formatDouble(avgElementLatency));
		}
		builder.append('\n');
		this.writer.write(builder.toString());
		this.writer.flush();
	}

	private String formatDouble(double doubleValue) {
		return String.format("%.2f", doubleValue);
	}

	private Object getLogTimestamp() {
		return ProfilingUtils.alignToInterval(System.currentTimeMillis(),
				this.loggingInterval) / 1000;
	}

	private void writeHeaders(ProfilingSequenceSummary summary)
			throws IOException {
		StringBuilder builder = new StringBuilder();
		builder.append("timestamp;");
		builder.append("noOfActivePaths;");
		builder.append("noOfInactivePaths;");
		builder.append("avgTotalPathLatency;");
		builder.append("minPathLatency;");
		builder.append("maxPathLatency");

		int nextEdgeIndex = 1;

		ProfilingSequence sequence = summary.getProfilingSequence();
		List<QosGroupVertex> groupVertices = sequence
				.getSequenceVertices();

		for (int i = 0; i < groupVertices.size(); i++) {
			QosGroupVertex groupVertex = groupVertices.get(i);

			boolean includeVertex = i == 0 && sequence.isIncludeStartVertex()
					|| i > 0 && i < groupVertices.size() - 1
					|| i == groupVertices.size() - 1
					&& sequence.isIncludeEndVertex();

			if (includeVertex) {
				builder.append(';');
				builder.append(groupVertex.getName());
			}

			QosGroupEdge forwardEdge = groupVertex.getForwardEdge();
			if (forwardEdge != null) {
				builder.append(';');
				builder.append("edge" + nextEdgeIndex + "obl");
				builder.append(';');
				builder.append("edge" + nextEdgeIndex);
				nextEdgeIndex++;
			}
		}
		builder.append('\n');
		this.writer.write(builder.toString());
		this.headersWritten = true;
	}
}
