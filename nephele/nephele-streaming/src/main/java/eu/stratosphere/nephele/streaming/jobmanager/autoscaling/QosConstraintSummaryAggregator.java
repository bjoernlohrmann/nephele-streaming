package eu.stratosphere.nephele.streaming.jobmanager.autoscaling;

import java.util.HashMap;
import java.util.Set;

import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.QosConstraintSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosManagerID;

public class QosConstraintSummaryAggregator {
	
	private final HashMap<QosManagerID, QosConstraintSummary> summaries = new HashMap<QosManagerID, QosConstraintSummary>();
	
	private final Set<QosManagerID> requiredManagerIDs;

	private final JobGraphLatencyConstraint constraint; 
	
	public QosConstraintSummaryAggregator(JobGraphLatencyConstraint constraint,
			Set<QosManagerID> managerIDs) {

		this.constraint = constraint;
		this.requiredManagerIDs = managerIDs;
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

		return aggregation;
	}
}
