package eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.message.action.SetOutputBufferLifetimeTargetAction;
import eu.stratosphere.nephele.streaming.taskmanager.StreamMessagingThread;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosConstraintViolationListener;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosSequenceLatencySummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.EdgeQosData;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphMember;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.List;

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

			ValueHistory<Integer> oblHistory = edge.getQosData().getTargetObltHistory();
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

		for (QosGraphMember member : sequenceMembers) {
			
			if (member.isVertex()) {
				continue;
			}

			QosEdge edge = (QosEdge) member;
			EdgeQosData qosData = edge.getQosData();

			if (qosData.isInChain()) {
				continue;
			}
			
			// stick to the 80-20 rule (80% of available time (minus 1 ms shipping time) for output buffering,
			// 20% for queueing) and trust in the autoscaler to keep the queueing time in check.
			int targetObl = Math.max(0, (int) (availLatPerChannel * 0.8 - 1));
			int targetOblt = qosData.proposeOutputBufferLifetimeForOutputBufferLatencyTarget(targetObl);
			
			// do nothing if change is very small
			ValueHistory<Integer> targetObltHistory = qosData.getTargetObltHistory();
			if (targetObltHistory.hasEntries()) {
				int oldTargetOblt = targetObltHistory.getLastEntry().getValue();
				
				if (oldTargetOblt == targetOblt) {
					continue;
				}
			}

			// do nothing target output buffer lifetime on this edge is already being adjusted to
			// a smaller value
			if (!edgesToAdjust.containsKey(edge)
					|| edgesToAdjust.get(edge) > targetOblt) {
				edgesToAdjust.put(qosData.getEdge(), targetOblt);
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

		SetOutputBufferLifetimeTargetAction action = new SetOutputBufferLifetimeTargetAction(
				this.jobID, edge.getOutputGate().getVertex().getID(), edge
						.getOutputGate().getGateID(),
				edge.getSourceChannelID(), targetObl);

		InstanceConnectionInfo receiver = edge.getOutputGate().getVertex()
				.getExecutingInstance();
		this.messagingThread.sendAsynchronously(receiver, action);
	}
}
