package eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers;

import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.message.action.SetOutputLatencyTargetAction;
import eu.stratosphere.nephele.streaming.taskmanager.StreamMessagingThread;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosConstraintViolationListener;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosSequenceLatencySummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.EdgeQosData;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphMember;

/**
 * Used by the Qos manager to manage output latencies in a Qos graph. It uses a
 * Qos model to search for sequences of Qos edges and vertices that violate a
 * Qos constraint, and then redefines target output buffer latencies
 * accordingly.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class OutputBufferLatencyManager {

	private static final Log LOG = LogFactory.getLog(OutputBufferLatencyManager.class);

	private final JobID jobID;
	
	private final StreamMessagingThread messagingThread;
	
	private final HashMap<QosEdge, Integer> edgesToAdjust = new HashMap<QosEdge, Integer>();
	
	private int staleSequencesCounter = 0;
	
	final QosConstraintViolationListener listener = new QosConstraintViolationListener() {
		@Override
		public void handleViolatedConstraint(
				JobGraphLatencyConstraint constraint,
				List<QosGraphMember> sequenceMembers,
				QosSequenceLatencySummary qosSummary) {

			if (qosSummary.isMemberQosDataFresh()) {
				collectEdgesToAdjust(constraint, sequenceMembers, qosSummary, edgesToAdjust);
			} else {
				staleSequencesCounter++;
			}
		}
	};

	public OutputBufferLatencyManager(JobID jobID) {
		this.jobID = jobID;
		this.messagingThread = StreamMessagingThread.getInstance();
	}

	public void applyAndSendBufferAdjustments(long oblHistoryTimestamp) throws InterruptedException {
		doAdjust(edgesToAdjust, oblHistoryTimestamp);
		
		LOG.debug(String.format(
				"Adjusted edges: %d | Sequences with stale Qos data: %d",
				edgesToAdjust.size(), staleSequencesCounter));
		edgesToAdjust.clear();
		staleSequencesCounter = 0;
	}
	
	public QosConstraintViolationListener getQosConstraintViolationListener() {
		return this.listener;
	}

	private void doAdjust(HashMap<QosEdge, Integer> edgesToAdjust, long oblHistoryTimestamp)
			throws InterruptedException {

		for (QosEdge edge : edgesToAdjust.keySet()) {
			int newTargetObl = edgesToAdjust.get(edge);

			ValueHistory<Integer> oblHistory = edge.getQosData()
					.getTargetOblHistory();
			oblHistory.addToHistory(oblHistoryTimestamp, newTargetObl);

			this.setTargetOutputBufferLatency(edge, newTargetObl);
		}
	}

	private void collectEdgesToAdjust(JobGraphLatencyConstraint constraint,
			List<QosGraphMember> sequenceMembers,
			QosSequenceLatencySummary qosSummary,
			HashMap<QosEdge, Integer> edgesToAdjust) {

		double availLatPerChannel = computeAvailableChannelLatency(
				constraint, qosSummary) / countUnchainedEdges(sequenceMembers);

		int i=-1;
		for (QosGraphMember member : sequenceMembers) {
			i++;
			
			if (member.isVertex()) {
				continue;
			}

			QosEdge edge = (QosEdge) member;
			EdgeQosData qosData = edge.getQosData();

			if (qosData.isInChain()) {
				continue;
			}
			
			double edgeTransportLatency = qosSummary.getMemberLatencies()[i][1];
			int targetObl;
			
			if(edgeTransportLatency > 0.2 * availLatPerChannel) {
				// queue wait is larger than 20% of available latency
				// per channel.  in this case stick to the 80:20 rule,
				// build in another 10% margin of safety, and
				// trust in the autoscaler to adjust parallelism
				// so that queue wait drops.
				targetObl = (int) (availLatPerChannel * 0.8 * 0.9);
			} else {
				// queue wait is lower than 20%. in this case use
				// the resulting "free" available channel latency
				// for output buffering. And as always build in
				// another 10% margin of safety.

				targetObl = (int) (0.9 * (availLatPerChannel - edgeTransportLatency));
			}

			// do nothing if change is very small
			ValueHistory<Integer> targetOblHistory = qosData.getTargetOblHistory();
			if (targetOblHistory.hasEntries()) {
				int oldTargetObl = targetOblHistory.getLastEntry().getValue();
				
				if (oldTargetObl == targetObl
						|| (oldTargetObl != 0 && (Math.abs(oldTargetObl
								- targetObl) / oldTargetObl) < 0.05)) {
					continue;
				}
			}

			// do nothing target output buffer latency on this edge is already being adjusted to
			// a smaller value
			if (!edgesToAdjust.containsKey(edge)
					|| edgesToAdjust.get(edge) > targetObl) {
				edgesToAdjust.put(qosData.getEdge(), targetObl);
			}
		}
	}

	private int countUnchainedEdges(List<QosGraphMember> sequenceMembers) {
		int unchainedCount = 0;
		for (QosGraphMember member : sequenceMembers) {
			if (member.isEdge() && !((QosEdge) member).getQosData().isInChain()) {
				unchainedCount++;
			}
		}
		return unchainedCount;
	}

	private double computeAvailableChannelLatency(JobGraphLatencyConstraint constraint,
			QosSequenceLatencySummary qosSummary) {

		double toReturn;

		if (constraint.getLatencyConstraintInMillis() > qosSummary.getVertexLatencySum()) {
			// regular case we have a chance of meeting the constraint by
			// adjusting output buffer sizes here and trusting
			// in the autoscaler to adjust transport latency
			toReturn = constraint.getLatencyConstraintInMillis() - qosSummary.getVertexLatencySum();

		} else {
			// overload case (vertexLatencySum >= constraint): we
			// don't have a chance of meeting the constraint at all
			toReturn = 0;
		}

		return toReturn;
	}

	private void setTargetOutputBufferLatency(QosEdge edge, int targetObl)
			throws InterruptedException {

		SetOutputLatencyTargetAction action = new SetOutputLatencyTargetAction(
				this.jobID, edge.getOutputGate().getVertex().getID(), edge
						.getOutputGate().getGateID(),
				edge.getSourceChannelID(), targetObl);

		InstanceConnectionInfo receiver = edge.getOutputGate().getVertex()
				.getExecutingInstance();
		this.messagingThread.sendAsynchronously(receiver, action);
	}
}
