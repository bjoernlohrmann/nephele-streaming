package eu.stratosphere.nephele.streaming.jobmanager.autoscaling;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.plugins.PluginManager;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;

public class CpuLoadLogger extends AbstractCpuLoadLogger {
	private static final String LOGFILE_PATTERN_KEY = PluginManager
			.prefixWithPluginNamespace("streaming.qosmanager.logging.cpu_load_statistics_filepattern");

	private static final String DEFAULT_LOGFILE_PATTERN = "/tmp/cpu_load_statistics_%s";

	private BufferedWriter writer;

	public CpuLoadLogger(ExecutionGraph execGraph, JobGraphLatencyConstraint constraint, long loggingInterval) throws IOException {
		super(execGraph, constraint, loggingInterval);
		
		String logFile = GlobalConfiguration.getString(LOGFILE_PATTERN_KEY, DEFAULT_LOGFILE_PATTERN);
		if (logFile.contains("%s")) {
			logFile = String.format(logFile, constraint.getID().toString());
		}
		this.writer = new BufferedWriter(new FileWriter(logFile));
		this.writeHeaders(execGraph);
	}

	@Override
	public void logCpuLoads(Map<JobVertexID, GroupVertexCpuLoadSummary> loadSummaries) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append(getLogTimestamp() / 1000);
		for(JobVertexID id : this.groupVertices) {
			GroupVertexCpuLoadSummary summary = loadSummaries.get(id);
			sb.append(';');
			sb.append(summary.getHighs());
			sb.append(';');
			sb.append(summary.getMediums());
			sb.append(';');
			sb.append(summary.getLows());
			sb.append(';');
			sb.append(summary.getUnknowns());
		}
		
		sb.append('\n');
		
		this.writer.write(sb.toString());
		this.writer.flush();
	}
	
	private void writeHeaders(ExecutionGraph execGraph) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append("timestamp");
		
		for(JobVertexID id : this.groupVertices) {
			String name = execGraph.getExecutionGroupVertex(id).getName();
			for(String type : new String[]{"high", "medium", "low", "unknown"}) {
				sb.append(';');
				sb.append(name);
				sb.append(':');
				sb.append(type);
			}
		}
		
		sb.append('\n');
		
		this.writer.write(sb.toString());
		this.writer.flush();
	}
	
	public void close() throws IOException {
		this.writer.close();
	}

}
