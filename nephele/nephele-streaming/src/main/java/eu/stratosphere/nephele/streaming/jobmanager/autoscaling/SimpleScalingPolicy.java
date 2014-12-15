package eu.stratosphere.nephele.streaming.jobmanager.autoscaling;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.jobmanager.autoscaling.optimization.GG1Server;
import eu.stratosphere.nephele.streaming.jobmanager.autoscaling.optimization.GG1ServerKingman;
import eu.stratosphere.nephele.streaming.jobmanager.autoscaling.optimization.Rebalancer;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosConstraintSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosGroupEdgeSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosGroupVertexSummary;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Scaling policy that scales individual group vertices based on their CPU load
 * and record sent-consumed ratios.
 *
 * @author Bjoern Lohrmann
 */
public class SimpleScalingPolicy extends AbstractScalingPolicy {

	private static final Log LOG = LogFactory.getLog(SimpleScalingPolicy.class);

	public SimpleScalingPolicy(
					ExecutionGraph execGraph,
					HashMap<LatencyConstraintID, JobGraphLatencyConstraint> qosConstraints) {
		super(execGraph, qosConstraints);
	}

	protected void getParallelismChangesForConstraint(JobGraphLatencyConstraint constraint,
	                                                  QosConstraintSummary constraintSummary,
	                                                  Map<JobVertexID, Integer> parallelismChanges)
					throws UnexpectedVertexExecutionStateException {

		ArrayList<GG1Server> servers = createServers(constraint, constraintSummary, parallelismChanges, true);
		if (hasBottleneck(servers)) {
			resolveBottleneck(parallelismChanges, servers);
		} else {
			rebalance(constraint, constraintSummary, parallelismChanges, servers);
		}
	}

//	private double getMeanLatency(JobGraphLatencyConstraint constraint, QosConstraintSummary constraintSummary) {
//		double meanSum=0;
//		
//		for (SequenceElement seqElem : constraint.getSequence()) {
//			if(seqElem.isVertex()) {
//				QosGroupVertexSummary vertexSummary = constraintSummary.getGroupVertexSummary(seqElem.getIndexInSequence());
//				meanSum += vertexSummary.getMeanVertexLatency();
//			} else {
//				QosGroupEdgeSummary edgeSummary = constraintSummary.getGroupEdgeSummary(seqElem.getIndexInSequence());
//				meanSum += edgeSummary.getOutputBufferLatencyMean();
//				meanSum += edgeSummary.getTransportLatencyMean();
//			}
//		}
//		
//		return meanSum;
//	}

	private void rebalance(JobGraphLatencyConstraint constraint,
	                       QosConstraintSummary constraintSummary,
	                       Map<JobVertexID, Integer> parallelismChanges,
	                       ArrayList<GG1Server> servers) {

		double targetQueueingTimeMillis = computeTargetQueueTimeOfElasticServers(constraint,
						constraintSummary, servers);

		Rebalancer reb = new Rebalancer(filterNonElasticServers(servers), targetQueueingTimeMillis);
		boolean rebalanceSuccess = reb.computeRebalancedParallelism();

		Map<JobVertexID, Integer> rebActions = reb.getScalingActions();
		Map<JobVertexID, Integer> rebParallelism = reb.getRebalancedParallelism();

		if (LOG.isDebugEnabled()) {
			// print some debug output
			StringBuilder strBuild = new StringBuilder();
			for (JobVertexID id : rebActions.keySet()) {
				strBuild.append(rebParallelism.get(id));
				strBuild.append(String.format("(%d)", rebActions.get(id)));
			}
			strBuild.append(String.format(" | rebalanceSuccess: %s | targetQueueTime:%.2fms | projectedQueueTime: %.2fms",
							Boolean.toString(rebalanceSuccess),
							targetQueueingTimeMillis,
							reb.getRebalancedQueueWait() * 1000));
			LOG.debug("Rebalance: " + strBuild.toString());
		}

		// "dampen" scale downs
//		for (JobVertexID id : rebActions.keySet()) {
//			if (rebActions.get(id) < 0) {
//				int diff = (int) Math.floor(-rebActions.get(id) * 0.25);
//				rebParallelism.put(id, rebParallelism.get(id) + diff);
//				rebActions.put(id, rebActions.get(id) + diff);
//				LOG.info(String.format("Rebalance: Limiting scale down of %s to %d", id.toString(), rebActions.get(id)));
//			}
//		}

		// filter out minuscule changes in parallelism
		for (GG1Server server : servers) {
			JobVertexID id = server.getGroupVertexID();

			if (!rebParallelism.containsKey(id)) {
				continue;
			}

			int newP = rebParallelism.get(id);
			if (parallelismChanges.containsKey(id)) {
				newP = Math.max(newP, parallelismChanges.get(id));
			}

			if (Math.abs(newP - server.getCurrentParallelism()) / ((double) server.getCurrentParallelism()) <= 0.04) {
				parallelismChanges.remove(id);
			} else {
				parallelismChanges.put(id, newP);
			}
		}
	}


	private ArrayList<GG1Server> filterNonElasticServers(
					ArrayList<GG1Server> servers) {

		ArrayList<GG1Server> ret = new ArrayList<GG1Server>();
		for (GG1Server server : servers) {
			if (server.isElastic()) {
				ret.add(server);
			}
		}
		return ret;
	}

	private ArrayList<GG1Server> createServers(JobGraphLatencyConstraint constraint,
	                                           QosConstraintSummary constraintSummary,
	                                           Map<JobVertexID, Integer> scalingActions,
	                                           boolean allowScaleDown) {

		ArrayList<GG1Server> gg1Servers = new ArrayList<GG1Server>();

		for (SequenceElement seqElem : constraint.getSequence()) {
			if (!seqElem.isEdge()) {
				continue;
			}

			ExecutionGroupVertex consumerGroupVertex = getExecutionGraph().getExecutionGroupVertex(
							seqElem.getTargetVertexID());
			QosGroupEdgeSummary edgeSummary = constraintSummary.getGroupEdgeSummary(seqElem.getIndexInSequence());
			JobVertexID id = consumerGroupVertex.getJobVertexID();

			int minParallelism;
			int maxParallelism;

			boolean isElastic = consumerGroupVertex.hasElasticNumberOfRunningSubtasks();
			if (isElastic) {
				int currParallelism = consumerGroupVertex.getCurrentElasticNumberOfRunningSubtasks();

				minParallelism = consumerGroupVertex.getMinElasticNumberOfRunningSubtasks();
				if (!allowScaleDown) {
					minParallelism = currParallelism;
				}

				if (scalingActions.get(id) != null) {
					minParallelism = Math.max(minParallelism,
									consumerGroupVertex.getCurrentElasticNumberOfRunningSubtasks() + scalingActions.get(id));
				}

				maxParallelism = consumerGroupVertex.getMaxElasticNumberOfRunningSubtasks();
			} else {
				minParallelism = consumerGroupVertex.getCurrentNumberOfGroupMembers();
				maxParallelism = minParallelism;
			}

			gg1Servers.add(new GG1ServerKingman(id, minParallelism, maxParallelism, edgeSummary));
		}

		return gg1Servers;
	}

	public double computeTargetQueueTimeOfElasticServers(JobGraphLatencyConstraint constraint,
	                                                     QosConstraintSummary constraintSummary,
	                                                     ArrayList<GG1Server> servers) {

		double availableShippingDelayMillis = constraint.getLatencyConstraintInMillis();
		double nonElasticShippingDelayMillis = 0;

		int serverIndex = 0;
		int elasticServersCount = 0;
		for (SequenceElement seqElem : constraint.getSequence()) {
			if (seqElem.isEdge()) {
				QosGroupEdgeSummary edgeSummary = constraintSummary.getGroupEdgeSummary(seqElem.getIndexInSequence());

				if (!servers.get(serverIndex).isElastic()) {
					nonElasticShippingDelayMillis += edgeSummary.getOutputBufferLatencyMean();
					nonElasticShippingDelayMillis += edgeSummary.getTransportLatencyMean();
				} else {
					elasticServersCount++;
				}

				serverIndex++;
			} else {
				QosGroupVertexSummary vertexSummary = constraintSummary.getGroupVertexSummary(seqElem
								.getIndexInSequence());
				availableShippingDelayMillis -= vertexSummary.getMeanVertexLatency();
			}
		}

		if (availableShippingDelayMillis - nonElasticShippingDelayMillis <= 0) {
			LOG.warn("Rebalance: Could not enforce constraint by scaling due to non-elastic (already 100% scaled-out) job vertices");
		}

		if (availableShippingDelayMillis <= 0) {
			// if availableShippingDelay is negative, we cannot enforce the
			// constraint by scaling. The only thing left to do is log a warning
			// and do graceful degradation. for graceful degradation we allow
			// 1ms of queueing delay at elastic servers.

			return elasticServersCount;
		} else {
			// availableShippingDelay right now is the time available for output
			// buffer latency AND queue waiting before elastic tasks. According
			// to the 80:20 rule, output buffer latency will adapt itself
			// to be 80% of that, so we can take the remaining 20% for queueing
			// (minus another 10% margin of safety).
			return (availableShippingDelayMillis - nonElasticShippingDelayMillis) * 0.2 * 0.9;
		}
	}

	/**
	 * This is a last resort technique, when a bottleneck has formed. With
	 * bottlenecks, queueing models are not applicable anymore. Resolve
	 * bottlenecks at least doubles the bottlneck's degree of parallelism, in
	 * the hope of resolving the bottleneck as fast as possible.
	 *
	 * @param parallelismChanges Currently planned (but not yet executed) changes in parallelism.
	 * @param servers            List of G/G/1 servers
	 */
	private void resolveBottleneck(Map<JobVertexID, Integer> parallelismChanges, ArrayList<GG1Server> servers) {
		for (GG1Server bottleneckServer : findBottleneckServers(servers)) {
			int currP = bottleneckServer.getCurrentParallelism();
			int doubleP = 2 * currP;
			int halfUtilizationP = (int) Math.ceil(2 * currP * bottleneckServer.getCurrentMeanUtilization());

			int maxP = bottleneckServer.getUpperBoundParallelism();

			int newP = Math.min(maxP, Math.max(doubleP, halfUtilizationP));

			if (newP > currP) {
				JobVertexID id = bottleneckServer.getGroupVertexID();
				if (parallelismChanges.containsKey(id)) {
					newP = Math.max(newP, parallelismChanges.get(id));
				}
				parallelismChanges.put(id, newP);
			} else {
				LOG.warn("ResolveBottleneck: Could not resolve bottleneck by scaling out, due to non-elastic (or already 100% scaled-out) job vertices");
			}
		}
	}

	private boolean hasBottleneck(ArrayList<GG1Server> servers) {
		return !findBottleneckServers(servers).isEmpty();
	}

	private ArrayList<GG1Server> findBottleneckServers(ArrayList<GG1Server> servers) {
		final double bottleneckUtilizationThreshold = 0.99;

		ArrayList<GG1Server> bottlenecks = new ArrayList<GG1Server>();

		for (GG1Server server : servers) {
			if (server.getCurrentMeanUtilization() >= bottleneckUtilizationThreshold) {
				bottlenecks.add(server);
			}
		}

		return bottlenecks;
	}
}
