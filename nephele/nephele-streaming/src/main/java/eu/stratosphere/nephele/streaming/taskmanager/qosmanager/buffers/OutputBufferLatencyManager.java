package eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers;

import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.plugins.PluginManager;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.message.action.SetOutputLatencyTargetAction;
import eu.stratosphere.nephele.streaming.taskmanager.StreamMessagingThread;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosConstraintViolationListener;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosSequenceLatencySummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosUtils;
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

	/**
	 * Provides access to the configuration entry which defines the time interval
	 * for adjusting target output buffer latencies.
	 */
	public static final String QOSMANAGER_ADJUSTMENTINTERVAL_KEY = PluginManager
			.prefixWithPluginNamespace("streaming.qosmanager.adjustmentinterval");

	public static final long DEFAULT_ADJUSTMENTINTERVAL = 5000;

	public final static long WAIT_BEFORE_FIRST_ADJUSTMENT = 10 * 1000;

	public long adjustmentInterval;

	private StreamMessagingThread messagingThread;

	private long timeOfNextAdjustment;

	private JobID jobID;
	
	final HashMap<QosEdge, Integer> edgesToAdjust = new HashMap<QosEdge, Integer>();
	
	int staleSequencesCounter = 0;
	
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

		this.adjustmentInterval = GlobalConfiguration.getLong(
				QOSMANAGER_ADJUSTMENTINTERVAL_KEY, DEFAULT_ADJUSTMENTINTERVAL);

		this.timeOfNextAdjustment = QosUtils.alignToInterval(
				System.currentTimeMillis() + WAIT_BEFORE_FIRST_ADJUSTMENT,
				this.adjustmentInterval);
	}

	public long getAdjustmentInterval() {
		return this.adjustmentInterval;
	}

	public void applyAndSendBufferAdjustments() throws InterruptedException {
		doAdjust(edgesToAdjust);
		
		LOG.debug(String.format(
				"Adjusted edges: %d | Sequences with stale Qos data: %d",
				edgesToAdjust.size(), staleSequencesCounter));
		edgesToAdjust.clear();
		staleSequencesCounter = 0;
		this.refreshTimeOfNextAdjustment();
	}
	
	public QosConstraintViolationListener getQosConstraintViolationListener() {
		return this.listener;
	}

	private void doAdjust(HashMap<QosEdge, Integer> edgesToAdjust)
			throws InterruptedException {

		for (QosEdge edge : edgesToAdjust.keySet()) {
			int newTargetObl = edgesToAdjust.get(edge);

			ValueHistory<Integer> oblHistory = edge.getQosData()
					.getTargetOblHistory();
			oblHistory.addToHistory(this.timeOfNextAdjustment, newTargetObl);

			this.setTargetOutputBufferLatency(edge, newTargetObl);
		}
	}

	private void refreshTimeOfNextAdjustment() {
		long now = System.currentTimeMillis();
		while (this.timeOfNextAdjustment <= now) {
			this.timeOfNextAdjustment += this.adjustmentInterval;
		}
	}

	private void collectEdgesToAdjust(JobGraphLatencyConstraint constraint,
			List<QosGraphMember> sequenceMembers,
			QosSequenceLatencySummary qosSummary,
			HashMap<QosEdge, Integer> edgesToAdjust) {

		int uniformTargetObl = computeUniformTargetObl(
				constraint, qosSummary);

		for (QosGraphMember member : sequenceMembers) {

			if (member.isVertex()) {
				continue;
			}

			QosEdge edge = (QosEdge) member;
			EdgeQosData qosData = edge.getQosData();

			if (qosData.isInChain()) {
				continue;
			}

			// do nothing if change is very small
			ValueHistory<Integer> targetOblHistory = qosData.getTargetOblHistory();
			if(targetOblHistory.hasEntries()) {
				int oldTargetObl = targetOblHistory.getLastEntry().getValue();
				if (Math.abs(oldTargetObl - uniformTargetObl) / oldTargetObl < 0.05) {
					continue;
				}
			}

			// do nothing target output buffer latency on this edge is already being adjusted to
			// a smaller value
			if (!edgesToAdjust.containsKey(edge)
					|| edgesToAdjust.get(edge) > uniformTargetObl) {
				edgesToAdjust.put(qosData.getEdge(), uniformTargetObl);
			}
		}
	}

	private int computeUniformTargetObl(
			JobGraphLatencyConstraint constraint, QosSequenceLatencySummary qosSummary) {

		if (constraint.getLatencyConstraintInMillis() > qosSummary.getNonOutputBufferLatency()) {
			// regular case (nonOutputBufferLatency < constraint): we have a
			// chance of meeting the constraint
			// by telling the TM to adjust output buffer sizes
			
			return (int) (constraint.getLatencyConstraintInMillis() - qosSummary
					.getNonOutputBufferLatency()) / qosSummary.getNoOfEdges();
		} else {
			// overload case (nonOutputBufferLatency >= constraint): we
			// don't have a chance of meeting the constraint by
			// adjusting output buffer sizes. in this case
			// choose a sensible default
			
			return (int) constraint.getLatencyConstraintInMillis() / qosSummary.getNoOfEdges();
		}
	}


	public boolean isAdjustmentNecessary(long now) {
		return now >= this.timeOfNextAdjustment;
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
