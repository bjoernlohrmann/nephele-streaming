package eu.stratosphere.nephele.streaming.jobmanager.autoscaling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.jobmanager.autoscaling.optimization.GG1Server;
import eu.stratosphere.nephele.streaming.jobmanager.autoscaling.optimization.Rebalancer;
import eu.stratosphere.nephele.streaming.message.TaskCpuLoadChange;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosConstraintSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosGroupEdgeSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosGroupVertexSummary;

/**
 * Scaling policy that scales individual group vertices based on their CPU load
 * and record sent-consumed ratios.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class SimpleScalingPolicy extends AbstractScalingPolicy {
	
	private static final Log LOG = LogFactory
			.getLog(SimpleScalingPolicy.class);

	public SimpleScalingPolicy(
			ExecutionGraph execGraph,
			HashMap<LatencyConstraintID, JobGraphLatencyConstraint> qosConstraints) {
		super(execGraph, qosConstraints);
	}

	protected void collectScalingActionsForConstraint(
			JobGraphLatencyConstraint constraint,
			QosConstraintSummary constraintSummary,
			Map<ExecutionVertexID, TaskCpuLoadChange> taskCpuLoads,
			LatencyConstraintCpuLoadSummary summarizedCpuUtilizations,
			Map<JobVertexID, Integer> scalingActions)
			throws UnexpectedVertexExecutionStateException {
		
		
		if(constraintSummary.getViolationReport().getMeanSequenceLatency() > constraint.getLatencyConstraintInMillis()) {
			// constraint is violated on average
			if(hasBottleneck(constraintSummary)) {
				resolveBottleneck(constraint, constraintSummary, scalingActions);
			} else {
				rebalance(constraint, constraintSummary, scalingActions);
			}
		} else {
			rebalance(constraint, constraintSummary, scalingActions);
		}
		
		LOG.debug(scalingActions.toString());
	}

	private void rebalance(JobGraphLatencyConstraint constraint,
			QosConstraintSummary constraintSummary,
			Map<JobVertexID, Integer> scalingActions) {
		
		
		ArrayList<GG1Server> gg1Servers = new ArrayList<GG1Server>();
		
		int elasticVertices = 0;

		// Computes available time for queuing by subtracting all unavoidable
		// latencies (processing time, queue wait before
		// of non-elastic vertices) from the constraint time. 
		double availableQueueTime = constraint.getLatencyConstraintInMillis();
		for (SequenceElement seqElem : constraint.getSequence()) {
			if (seqElem.isEdge()) {
				ExecutionGroupVertex consumerGroupVertex = getExecutionGraph()
						.getExecutionGroupVertex(seqElem.getTargetVertexID());

				QosGroupEdgeSummary edgeSummary = constraintSummary.getGroupEdgeSummary(seqElem.getIndexInSequence());
				
				if (!consumerGroupVertex.hasElasticNumberOfRunningSubtasks()) {
					availableQueueTime -= edgeSummary.getTransportLatencyMean();
					continue;
				}
				
				elasticVertices++;
				
				gg1Servers.add(new GG1Server(consumerGroupVertex
						.getJobVertexID(), consumerGroupVertex
						.getMinElasticNumberOfRunningSubtasks(),
						consumerGroupVertex.getMaxElasticNumberOfRunningSubtasks(),
						edgeSummary));
				
			} else {
				QosGroupVertexSummary vertexSummary = constraintSummary.getGroupVertexSummary(seqElem.getIndexInSequence());
				availableQueueTime -= vertexSummary.getMeanVertexLatency();
			}
		}
		
		// So availableQueueTime is now the max time available for output buffer latency and queue waiting
		// before elastic tasks. Output buffer latency will "magically" adapts itself, however we
		// reserve 75% of the remaining availableQueueTime for it.
		availableQueueTime *= 0.25 * 0.8;
		
		Rebalancer reb = new Rebalancer(gg1Servers, availableQueueTime);
		
		if (availableQueueTime > 0 && reb.computeRebalancedParallelism()) {
			LOG.debug("RestoreConstraint: " + reb.getScalingActions());
			scalingActions.putAll(reb.getScalingActions());
		} else {
			LOG.warn(String.format("Could not find a suitable scale-out for the current situation (available queue time: %.3fms)", availableQueueTime));
		}
	}

	private void resolveBottleneck(JobGraphLatencyConstraint constraint, QosConstraintSummary constraintSummary, Map<JobVertexID, Integer> scalingActions) {
		// TODO Auto-generated method stub
	}

	private boolean hasBottleneck(QosConstraintSummary constraintSummary) {
		// TODO Auto-generated method stub
		return false;
	}
}
