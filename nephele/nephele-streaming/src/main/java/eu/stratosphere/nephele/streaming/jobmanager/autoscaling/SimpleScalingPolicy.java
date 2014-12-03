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
import eu.stratosphere.nephele.streaming.jobmanager.autoscaling.optimization.GG1ServerKingman;
import eu.stratosphere.nephele.streaming.jobmanager.autoscaling.optimization.Rebalancer;
import eu.stratosphere.nephele.streaming.message.TaskCpuLoadChange;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosConstraintSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosGroupEdgeSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosGroupVertexSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosUtils;
import eu.stratosphere.nephele.streaming.util.StreamPluginConfig;

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
				rebalance(constraint, constraintSummary, scalingActions, false);
			}
		} else {
			rebalance(constraint, constraintSummary, scalingActions, true);
		}
		
		LOG.debug(String.format("%d %s", QosUtils.alignToInterval(
				System.currentTimeMillis(),
				StreamPluginConfig.getAdjustmentIntervalMillis()) / 1000,
				scalingActions.toString()));
	}

	private void rebalance(JobGraphLatencyConstraint constraint,
			QosConstraintSummary constraintSummary,
			Map<JobVertexID, Integer> scalingActions,
			boolean allowScaleDown) {
		
		ArrayList<GG1Server> gg1Servers = new ArrayList<GG1Server>();
		
		// Computes available time for queueing by subtracting all unavoidable
		// latencies (processing time, queue wait before
		// of non-elastic vertices) from the constraint time. 
		double availableQueueTime = constraint.getLatencyConstraintInMillis();
		
		for (SequenceElement seqElem : constraint.getSequence()) {
			if (seqElem.isEdge()) {
				ExecutionGroupVertex consumerGroupVertex = getExecutionGraph()
						.getExecutionGroupVertex(seqElem.getTargetVertexID());

				QosGroupEdgeSummary edgeSummary = constraintSummary.getGroupEdgeSummary(seqElem.getIndexInSequence());
				
				if (consumerGroupVertex.hasElasticNumberOfRunningSubtasks()) {
					
					int minParallelism = consumerGroupVertex
							.getMinElasticNumberOfRunningSubtasks();
					
					if (!allowScaleDown) {
						minParallelism = edgeSummary.getActiveConsumerVertices();
					}
					
					gg1Servers.add(new GG1ServerKingman(consumerGroupVertex
							.getJobVertexID(), minParallelism,
							consumerGroupVertex.getMaxElasticNumberOfRunningSubtasks(),
							edgeSummary));
				} else {
					availableQueueTime -= edgeSummary.getOutputBufferLatencyMean();
					availableQueueTime -= edgeSummary.getTransportLatencyMean();
				}
			} else {
				QosGroupVertexSummary vertexSummary = constraintSummary.getGroupVertexSummary(seqElem.getIndexInSequence());
				availableQueueTime -= vertexSummary.getMeanVertexLatency();
			}
		}
		
		// So availableQueueTime right now is the time available for output
		// buffer latency AND queue waiting before elastic tasks. According
		// to the 80:20 rule, output buffer latency will adapt itself
		// to be 80% of that, so we can take the remaining 20% for queueing
		// (minus another 5% margin of safety).
		availableQueueTime *= 0.2 * 0.95;
		
		Rebalancer reb = new Rebalancer(gg1Servers, availableQueueTime);
		boolean rebalanceSuccess = false;
		
		if (availableQueueTime > 0) {
			rebalanceSuccess = reb.computeRebalancedParallelism();
			Map<JobVertexID, Integer> rebActions = reb.getScalingActions();
			Map<JobVertexID, Integer> rebParallelism = reb.getRebalancedParallelism();
			
			StringBuilder strBuild = new StringBuilder();
			for(JobVertexID id : rebActions.keySet()) {
				strBuild.append(rebParallelism.get(id));
				strBuild.append(String.format("(%d)", rebActions.get(id)));
			}
			
			LOG.debug("Rebalance: " + strBuild.toString());
			scalingActions.putAll(rebActions);
		}
		
		if (!rebalanceSuccess) {
			LOG.warn(String.format("Could not find a suitable scale-out for the current situation (available queue time: %.3fms)",
							availableQueueTime));
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
