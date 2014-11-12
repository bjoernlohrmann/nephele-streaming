package eu.stratosphere.nephele.streaming.web;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.web.QosStatisticsServlet;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.jobmanager.autoscaling.CpuLoadInMemoryLogger;
import eu.stratosphere.nephele.streaming.jobmanager.autoscaling.LatencyConstraintCpuLoadSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosInMemoryLogger;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.QosConstraintSummary;

public class QosJobWebStatistic extends QosStatisticsServlet.JobStatistic {
	
	private final static Log LOG = LogFactory.getLog(QosJobWebStatistic.class);
	
	private final JobID jobId;

	private final String jobName;
	
	private final long jobCreationTimestamp;

	private final long loggingInterval;
		
	private final Map<LatencyConstraintID, JobGraphLatencyConstraint> qosConstraints;
	
	private final HashMap<LatencyConstraintID, QosInMemoryLogger> qosMemoryLogger = new HashMap<LatencyConstraintID, QosInMemoryLogger>();
	
	private final HashMap<LatencyConstraintID, CpuLoadInMemoryLogger> cpuLoadMemoryLogger = new HashMap<LatencyConstraintID, CpuLoadInMemoryLogger>();


	public QosJobWebStatistic(ExecutionGraph execGraph, long loggingInterval, 
			Map<LatencyConstraintID, JobGraphLatencyConstraint> qosConstraints) {

		this.jobId = execGraph.getJobID();
		this.jobName = execGraph.getJobName();
		this.jobCreationTimestamp = System.currentTimeMillis();
		this.qosConstraints = qosConstraints;
		this.loggingInterval = loggingInterval;
		
		for(JobGraphLatencyConstraint constraint : qosConstraints.values()) {
			try {
				qosMemoryLogger.put(constraint.getID(), new QosInMemoryLogger(execGraph, constraint, loggingInterval));
				cpuLoadMemoryLogger.put(constraint.getID(), new CpuLoadInMemoryLogger(execGraph, constraint, loggingInterval));
			} catch(JSONException e) {
				LOG.error("Memory logger initialization failed!", e);
			}
		}
	}

	@Override
	public long getCreationTimestamp() {
		return this.jobCreationTimestamp;
	}

	@Override
	public long getRefreshInterval() {
		return this.loggingInterval;
	}

	@Override
	public int getMaxEntriesCount() {
		return qosMemoryLogger.values().iterator().next().getMaxEntriesCount();
	}

	public void logConstraintSummaries(List<QosConstraintSummary> constraintSummaries) {
		for (QosConstraintSummary constraintSummary : constraintSummaries) {
			LatencyConstraintID constraintId = constraintSummary.getLatencyConstraintID();

			try {
				if (this.qosMemoryLogger.containsKey(constraintId))
					this.qosMemoryLogger.get(constraintId).logSummary(constraintSummary);

			} catch (Exception e) {
				LOG.error("Error during QoS logging", e);
			}
		}
	}
	
	public void logCpuLoadSummaries(Map<LatencyConstraintID, LatencyConstraintCpuLoadSummary> summaries) {
		for(LatencyConstraintID constraintId : summaries.keySet()) {
			try {
				
				if (this.cpuLoadMemoryLogger.containsKey(constraintId))
					this.cpuLoadMemoryLogger.get(constraintId).logCpuLoads(summaries.get(constraintId));
				
			} catch(Exception e) {
				LOG.error("Error during cpu load logging", e);
			}
		}
	}

	@Override
	public JobID getJobId() {
		return this.jobId;
	}

	@Override
	public JSONObject getJobMetadata() throws JSONException {
		JSONObject meta = new JSONObject();
		meta.put("name", this.jobName);
		meta.put("creationTimestamp", this.jobCreationTimestamp);
		meta.put("creationFormatted", new Date(this.jobCreationTimestamp));
		return meta;
	}

	@Override
	public JSONObject getStatistics(JSONObject jobJson) throws JSONException {
		return getStatistics(jobJson, -1);
	}

	@Override
	public JSONObject getStatistics(JSONObject jobJson, long startTimestamp) throws JSONException {
		JSONObject constraints = new JSONObject();
		
		for(LatencyConstraintID id : this.qosConstraints.keySet()) {
			JSONObject constraint = new JSONObject();
			constraint.put("name", this.qosConstraints.get(id).getName());

			if (startTimestamp > 0) {
				this.qosMemoryLogger.get(id).toJson(constraint, startTimestamp);
				this.cpuLoadMemoryLogger.get(id).toJson(constraint, startTimestamp);
			} else {
				this.qosMemoryLogger.get(id).toJson(constraint);
				this.cpuLoadMemoryLogger.get(id).toJson(constraint);
			}
			
			constraints.put(id.toString(), constraint);
		}
		
		jobJson.put("constraints", constraints);
		return jobJson;
	}	
}
