package eu.stratosphere.nephele.streaming.jobmanager.autoscaling;

import java.util.HashMap;
import java.util.Set;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.QosConstraintSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosManagerID;

public class QosConstraintSummaryAggregator {
	
	private final HashMap<QosManagerID, QosConstraintSummary> summaries = new HashMap<QosManagerID, QosConstraintSummary>();
	
	private final Set<QosManagerID> requiredManagerIDs;

	private final ExecutionGraph executionGraph;

	private final JobGraphLatencyConstraint constraint;

	private final JobVertexID groupVertecies[];

	public QosConstraintSummaryAggregator(ExecutionGraph executionGraph, JobGraphLatencyConstraint constraint, Set<QosManagerID> managerIDs) {

		this.executionGraph = executionGraph;
		this.constraint = constraint;
		this.requiredManagerIDs = managerIDs;
		this.groupVertecies = constraint.getSequence().getVerticesForSequenceOrdered(true).toArray(new JobVertexID[0]);
	}
	
	public JobGraphLatencyConstraint getConstraint() {
		return constraint;
	}

	public void add(QosManagerID qosManagerID, QosConstraintSummary summary) {
		summaries.put(qosManagerID, summary);
	}
	
	public boolean canAggregate() {
		return this.summaries.size() == this.requiredManagerIDs.size();
	}
	
	public QosConstraintSummary computeAggregation() {
		QosConstraintSummary aggregation = new QosConstraintSummary(constraint);

		for (QosConstraintSummary summary : summaries.values()) {
			aggregation.mergeOtherSummary(summary);
		}

		int taskDop[] = new int[this.groupVertecies.length];
		int vertexIndex = 0;
		for (JobVertexID id : groupVertecies) {
			taskDop[vertexIndex] = this.executionGraph.getExecutionGroupVertex(id).getNumberOfRunningSubstasks();
			vertexIndex++;
		}
		aggregation.setTaskDop(taskDop);

		return aggregation;
	}
}
