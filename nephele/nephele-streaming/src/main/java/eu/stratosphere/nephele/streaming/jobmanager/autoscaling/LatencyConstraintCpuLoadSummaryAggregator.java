package eu.stratosphere.nephele.streaming.jobmanager.autoscaling;

import java.util.Map;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.message.TaskCpuLoadChange;

public class LatencyConstraintCpuLoadSummaryAggregator {

	private final ExecutionGraph execGraph;

	private final JobGraphLatencyConstraint qosConstraint;

	public LatencyConstraintCpuLoadSummaryAggregator(ExecutionGraph execGraph, JobGraphLatencyConstraint qosConstraint) {
		this.execGraph = execGraph;
		this.qosConstraint = qosConstraint;
	}

	public LatencyConstraintCpuLoadSummary summarizeCpuUtilizations(Map<ExecutionVertexID, TaskCpuLoadChange> taskCpuLoads)
			throws UnexpectedVertexExecutionStateException {

		LatencyConstraintCpuLoadSummary summary = new LatencyConstraintCpuLoadSummary();

		for (SequenceElement seqElem : qosConstraint.getSequence()) {
			if (seqElem.isEdge()) {

				JobVertexID sourceVertexID = seqElem.getSourceVertexID();
				if (!summary.containsKey(sourceVertexID)) {
					summary.put(sourceVertexID,
							new GroupVertexCpuLoadSummary(taskCpuLoads, execGraph.getExecutionGroupVertex(sourceVertexID)));
				}

				JobVertexID targetVertexID = seqElem.getTargetVertexID();
				if (!summary.containsKey(targetVertexID)) {
					summary.put(targetVertexID,
							new GroupVertexCpuLoadSummary(taskCpuLoads, execGraph.getExecutionGroupVertex(targetVertexID)));
				}

			}
		}

		return summary;
	}
}
