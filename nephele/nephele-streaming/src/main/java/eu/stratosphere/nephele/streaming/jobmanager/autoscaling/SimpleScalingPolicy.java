package eu.stratosphere.nephele.streaming.jobmanager.autoscaling;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.message.CpuLoadClassifier;
import eu.stratosphere.nephele.streaming.message.CpuLoadClassifier.CpuLoad;
import eu.stratosphere.nephele.streaming.message.TaskCpuLoadChange;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.QosConstraintSummary;

/**
 * Scaling policy that scales individual group vertices based on their CPU load
 * and record sent-consumed ratios.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class SimpleScalingPolicy extends AbstractScalingPolicy {

	public SimpleScalingPolicy(
			ExecutionGraph execGraph,
			HashMap<LatencyConstraintID, JobGraphLatencyConstraint> qosConstraints) {
		super(execGraph, qosConstraints);
	}

	protected void collectScalingActionsForConstraint(
			JobGraphLatencyConstraint constraint,
			QosConstraintSummary constraintSummary,
			Map<ExecutionVertexID, TaskCpuLoadChange> taskCpuLoads,
			Map<JobVertexID, Integer> scalingActions)
			throws UnexpectedVertexExecutionStateException {

		Map<JobVertexID, Double> summarizedCpuUtilizations = summarizeCpuUtilizations(
				constraint, taskCpuLoads);

		for (SequenceElement<JobVertexID> seqElem : constraint.getSequence()) {
			if (seqElem.isEdge()) {

				ExecutionGroupVertex senderGroupVertex = getExecutionGraph()
						.getExecutionGroupVertex(seqElem.getSourceVertexID());
				ExecutionGroupVertex consumerGroupVertex = getExecutionGraph()
						.getExecutionGroupVertex(seqElem.getTargetVertexID());

				if (!consumerGroupVertex.hasElasticNumberOfRunningSubtasks()) {
					continue;
				}

				double[][] memberStats = constraintSummary
						.getAggregatedMemberStatistics();
				double recordSendRate = memberStats[seqElem
						.getIndexInSequence()][2]
						* getNoOfRunningTasks(senderGroupVertex);
				double recordConsumptionRate = memberStats[seqElem
						.getIndexInSequence()][3]
						* getNoOfRunningTasks(consumerGroupVertex);

				double consumerCpuUtilPercent = summarizedCpuUtilizations
						.get(seqElem.getTargetVertexID());

				CpuLoad cpuLoad = CpuLoadClassifier
						.fromCpuUtilization(consumerCpuUtilPercent);

				if (cpuLoad == CpuLoad.HIGH
						&& (100 * recordSendRate / recordConsumptionRate) >= CpuLoadClassifier.HIGH_THRESHOLD_PERCENT) {

					// in general, scale up if the consumer task's avg cpu
					// utilization is high, with one exception:
					// don't do anything, if the sender has already
					// significantly reduced its sending rate but the
					// consumer task is busy working off already queued data. In
					// this case it is better
					// not to scale out and just wait until the queued data has
					// been processed.

					addScaleUpAction(seqElem, constraintSummary, scalingActions);

				} else if (cpuLoad == CpuLoad.LOW
						&& (100 * recordSendRate / recordConsumptionRate) >= CpuLoadClassifier.HIGH_THRESHOLD_PERCENT) {

					addScaleDownAction(seqElem, constraintSummary,
							consumerCpuUtilPercent / 100.0, scalingActions);
				}
			}
		}
	}

	private void addScaleUpAction(SequenceElement<JobVertexID> edge,
			QosConstraintSummary constraintSummary,
			Map<JobVertexID, Integer> scalingActions) {

		// midpoint between medium and high cpu load thresholds
		double targetCpuUtil = (CpuLoadClassifier.MEDIUM_THRESHOLD_PERCENT + CpuLoadClassifier.HIGH_THRESHOLD_PERCENT) / 200.0;

		int noOfSenderTasks = getNoOfRunningTasks(getExecutionGraph()
				.getExecutionGroupVertex(edge.getSourceVertexID()));

		double[] edgeStats = constraintSummary.getAggregatedMemberStatistics()[edge
				.getIndexInSequence()];
		double avgSendRate = edgeStats[2];
		double avgConsumeRate = edgeStats[3];

		// compute new number of consumer tasks so that future cpu utilization
		// will be close to targetCpuUtil (assuming perfect load balancing,
		// constant send rate and constant consumer capacity :-P ).
		int newNoOfConsumerTasks = (int) Math
				.ceil((avgSendRate * noOfSenderTasks)
						/ (avgConsumeRate * targetCpuUtil));

		// apply user defined limits
		ExecutionGroupVertex consumer = getExecutionGraph()
				.getExecutionGroupVertex(edge.getTargetVertexID());
		newNoOfConsumerTasks = applyElasticityLimits(consumer,
				newNoOfConsumerTasks);

		// merge with possibly existing scaling action from another constraint
		int scalingAction = newNoOfConsumerTasks
				- getNoOfRunningTasks(consumer);
		if (scalingAction != 0) {
			if (scalingActions.containsKey(consumer.getJobVertexID())) {
				scalingAction = Math.max(scalingAction,
						scalingActions.get(consumer.getJobVertexID()));
			}
			scalingActions.put(consumer.getJobVertexID(), scalingAction);
		}
	}

	private void addScaleDownAction(SequenceElement<JobVertexID> edge,
			QosConstraintSummary constraintSummary, double consumerCpuUtil,
			Map<JobVertexID, Integer> scalingActions) {

		// midpoint between medium and high cpu load thresholds
		double targetCpuUtil = (CpuLoadClassifier.MEDIUM_THRESHOLD_PERCENT + CpuLoadClassifier.HIGH_THRESHOLD_PERCENT) / 200.0;

		ExecutionGroupVertex consumer = getExecutionGraph()
				.getExecutionGroupVertex(edge.getTargetVertexID());

		int noOfConsumerTasks = getNoOfRunningTasks(consumer);

		double avgConsumeRate = constraintSummary
				.getAggregatedMemberStatistics()[edge.getIndexInSequence()][3];

		double loadFactor = (consumerCpuUtil * noOfConsumerTasks)
				/ avgConsumeRate;

		int newNoOfConsumerTasks = (int) Math.ceil(loadFactor * avgConsumeRate
				/ targetCpuUtil);

		// apply user defined limits
		newNoOfConsumerTasks = applyElasticityLimits(consumer,
				newNoOfConsumerTasks);

		// merge with possibly existing scaling action from another constraint
		int scalingAction = newNoOfConsumerTasks
				- getNoOfRunningTasks(consumer);
		if (scalingAction != 0) {
			if (scalingActions.containsKey(consumer.getJobVertexID())) {
				scalingAction = Math.min(scalingAction,
						scalingActions.get(consumer.getJobVertexID()));
			}

			scalingActions.put(consumer.getJobVertexID(), scalingAction);
		}
	}
}
