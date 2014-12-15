package eu.stratosphere.nephele.streaming.jobmanager.autoscaling;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosConstraintSummary;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract superclass of scaling policies providing commonly used
 * functionality.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public abstract class AbstractScalingPolicy {

	private final ExecutionGraph execGraph;

	private final HashMap<LatencyConstraintID, JobGraphLatencyConstraint> qosConstraints;

	public AbstractScalingPolicy(
			ExecutionGraph execGraph,
			HashMap<LatencyConstraintID, JobGraphLatencyConstraint> qosConstraints) {

		this.execGraph = execGraph;
		this.qosConstraints = qosConstraints;
	}

	public Map<JobVertexID, Integer> getParallelismChanges(List<QosConstraintSummary> constraintSummaries)
			throws UnexpectedVertexExecutionStateException {

		Map<JobVertexID, Integer> parallelismChanges = new HashMap<JobVertexID, Integer>();

		for (QosConstraintSummary constraintSummary : constraintSummaries) {
			if (constraintSummary.hasData()) {
				getParallelismChangesForConstraint(qosConstraints.get(constraintSummary.getLatencyConstraintID()), constraintSummary, parallelismChanges);
			}
		}

		return parallelismChanges;
	}

	protected abstract void getParallelismChangesForConstraint(
			JobGraphLatencyConstraint constraint,
			QosConstraintSummary constraintSummary,
			Map<JobVertexID, Integer> scalingActions)
			throws UnexpectedVertexExecutionStateException;

	protected ExecutionGraph getExecutionGraph() {
		return execGraph;
	}

	protected Map<LatencyConstraintID, JobGraphLatencyConstraint> getConstraints() {
		return qosConstraints;
	}
}
