package eu.stratosphere.nephele.streaming.jobmanager.autoscaling;

import java.util.HashMap;

import eu.stratosphere.nephele.jobgraph.JobVertexID;

public class LatencyConstraintCpuLoadSummary extends HashMap<JobVertexID, GroupVertexCpuLoadSummary> {
	private static final long serialVersionUID = 1L;

}
