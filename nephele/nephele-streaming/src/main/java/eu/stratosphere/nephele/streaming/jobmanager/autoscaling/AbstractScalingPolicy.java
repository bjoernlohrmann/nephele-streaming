package eu.stratosphere.nephele.streaming.jobmanager.autoscaling;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.message.TaskCpuLoadChange;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.QosConstraintSummary;

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

	public Map<JobVertexID, Integer> getScalingActions(
			List<QosConstraintSummary> constraintSummaries,
			Map<ExecutionVertexID, TaskCpuLoadChange> taskCpuLoads)
			throws UnexpectedVertexExecutionStateException {

		Map<JobVertexID, Integer> scalingActions = new HashMap<JobVertexID, Integer>();

		for (QosConstraintSummary constraintSummary : constraintSummaries) {
			collectScalingActionsForConstraint(
					qosConstraints.get(constraintSummary
							.getLatencyConstraintID()), constraintSummary,
					taskCpuLoads, scalingActions);
		}

		return scalingActions;
	}

	protected abstract void collectScalingActionsForConstraint(
			JobGraphLatencyConstraint constraint,
			QosConstraintSummary constraintSummary,
			Map<ExecutionVertexID, TaskCpuLoadChange> taskCpuLoads,
			Map<JobVertexID, Integer> scalingActions)
			throws UnexpectedVertexExecutionStateException;

	protected int applyElasticityLimits(ExecutionGroupVertex groupVertex,
			int newNoOfSubtasks) {

		return Math.max(
				groupVertex.getMinElasticNumberOfRunningSubtasks(),
				Math.min(newNoOfSubtasks,
						groupVertex.getMaxElasticNumberOfRunningSubtasks()));
	}

	protected Map<JobVertexID, GroupVertexCpuLoadSummary> summarizeCpuUtilizations(
			JobGraphLatencyConstraint constraint,
			Map<ExecutionVertexID, TaskCpuLoadChange> taskCpuLoads)
			throws UnexpectedVertexExecutionStateException {

		Map<JobVertexID, GroupVertexCpuLoadSummary> toReturn = new HashMap<JobVertexID, GroupVertexCpuLoadSummary>();

		for (SequenceElement<JobVertexID> seqElem : constraint.getSequence()) {
			if (seqElem.isEdge()) {

				JobVertexID sourceVertexID = seqElem.getSourceVertexID();
				if (!toReturn.containsKey(sourceVertexID)) {
					toReturn.put(
							sourceVertexID,
							new GroupVertexCpuLoadSummary(taskCpuLoads, execGraph
									.getExecutionGroupVertex(sourceVertexID)));
				}

				JobVertexID targetVertexID = seqElem.getTargetVertexID();
				if (!toReturn.containsKey(targetVertexID)) {
					toReturn.put(
							targetVertexID,
							new GroupVertexCpuLoadSummary(taskCpuLoads, execGraph
									.getExecutionGroupVertex(targetVertexID)));
				}

			}
		}

		return toReturn;
	}


	protected int getNoOfRunningTasks(ExecutionGroupVertex sendingGroupVertex) {
		int noOfSendingTasks;
		if (sendingGroupVertex.hasElasticNumberOfRunningSubtasks()) {
			noOfSendingTasks = sendingGroupVertex
					.getCurrentElasticNumberOfRunningSubtasks();
		} else {
			noOfSendingTasks = sendingGroupVertex
					.getCurrentNumberOfGroupMembers();
		}
		return noOfSendingTasks;
	}

	protected ExecutionGraph getExecutionGraph() {
		return execGraph;
	}

	protected Map<LatencyConstraintID, JobGraphLatencyConstraint> getConstraints() {
		return qosConstraints;
	}
}
