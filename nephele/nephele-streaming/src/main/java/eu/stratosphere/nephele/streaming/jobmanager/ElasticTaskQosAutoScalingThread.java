package eu.stratosphere.nephele.streaming.jobmanager;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.message.AbstractQosMessage;
import eu.stratosphere.nephele.streaming.message.AbstractSerializableQosMessage;
import eu.stratosphere.nephele.streaming.message.QosManagerConstraintSummaries;
import eu.stratosphere.nephele.streaming.message.TaskLoadStateChange;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.BufferSizeManager;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.QosConstraintSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;

public class ElasticTaskQosAutoScalingThread extends Thread {

	private static final Log LOG = LogFactory
			.getLog(ElasticTaskQosAutoScalingThread.class);

	private final LinkedBlockingQueue<AbstractSerializableQosMessage> qosMessages = new LinkedBlockingQueue<AbstractSerializableQosMessage>();

	private final HashMap<LatencyConstraintID, QosGraph> qosGraphs;

	/**
	 * Each QosManager reports a {@link QosConstraintSummary} in regular time
	 * intervals. The {@link QosConstraintSummary} objects are the result of
	 * aggregating all the {@link QosConstraintSummary} objects belonging to the
	 * same constraint.
	 */
	private final HashMap<LatencyConstraintID, QosConstraintSummary> aggregatedConstraintSummaries = new HashMap<LatencyConstraintID, QosConstraintSummary>();

	private final HashMap<ExecutionVertexID, TaskLoadStateChange> currentTaskLoads = new HashMap<ExecutionVertexID, TaskLoadStateChange>();

	private long timeOfLastScaling;

	private long timeOfNextScaling;

	public ElasticTaskQosAutoScalingThread(
			HashMap<LatencyConstraintID, QosGraph> qosGraphs) {

		this.setName("QosAutoScalingThread");
		this.qosGraphs = qosGraphs;
		this.timeOfLastScaling = 0;
		this.timeOfNextScaling = 0;
		this.start();
	}

	@Override
	public void run() {
		try {
			LOG.info("Qos Auto Scaling Thread started");
			
			long now = System.currentTimeMillis();

			while (true) {
				processMessages();
				Thread.sleep(500);

				now = System.currentTimeMillis();

				if (scalingIsDue(now)) {
					doAutoscale();

					timeOfLastScaling = System.currentTimeMillis();
					timeOfNextScaling = timeOfLastScaling
							+ GlobalConfiguration
									.getLong(
											BufferSizeManager.QOSMANAGER_ADJUSTMENTINTERVAL_KEY,
											BufferSizeManager.DEFAULT_ADJUSTMENTINTERVAL);
				}
			}
		} catch (InterruptedException e) {
			// do nothing
		} catch (Exception e) {
			LOG.error("Exception in auto scaling thread", e);
		} finally {
			cleanUp();
		}
	}

	private void doAutoscale() {			
		for(QosConstraintSummary constraintSummary : aggregatedConstraintSummaries.values()) {
			// FIXME implement autoscaling algorithm
			// determine load state on constrained subgraph (LOW, MEDIUM or HIGH)
			// if load state is LOW:
			//   execute scale down policy
			// else if load state is HIGH:
			//   execute scale up policy

			constraintSummary.reset();
		}		
	}

	private void cleanUp() {
		// clear large memory structures
		this.qosGraphs.clear();
		this.aggregatedConstraintSummaries.clear();
		this.currentTaskLoads.clear();
	}

	private boolean scalingIsDue(long now) {
		if (now < timeOfNextScaling) {
			return false;
		}

		for (LatencyConstraintID constraintID : qosGraphs.keySet()) {
			QosConstraintSummary constraintSummary = aggregatedConstraintSummaries
					.get(constraintID);

			if (constraintSummary == null || !constraintSummary.hasData()) {
				return false;
			}
		}

		return true;
	}

	private void processMessages() {
		while (!qosMessages.isEmpty()) {
			AbstractQosMessage nextMessage = qosMessages.poll();
			if (nextMessage instanceof TaskLoadStateChange) {
				handleTaskLoadStateChange((TaskLoadStateChange) nextMessage);
			} else if (nextMessage instanceof QosManagerConstraintSummaries) {
				handleQosManagerConstraintSummaries((QosManagerConstraintSummaries) nextMessage);
			}
		}
	}

	private void handleQosManagerConstraintSummaries(
			QosManagerConstraintSummaries nextMessage) {

		for (QosConstraintSummary constraintSummary : nextMessage
				.getConstraintSummaries()) {
			
			LatencyConstraintID constraintID = constraintSummary
					.getLatencyConstraintID();

			QosConstraintSummary aggregatedConstraintSummary = aggregatedConstraintSummaries
					.get(constraintID);

			if (aggregatedConstraintSummary == null) {
				aggregatedConstraintSummary = new QosConstraintSummary(
						constraintID,
						constraintSummary.getLatencyConstraintMillis(),
						constraintSummary.getSequenceLength(),
						constraintSummary.doesSequenceStartWithVertex());

				aggregatedConstraintSummaries.put(constraintID,
						aggregatedConstraintSummary);
			}

			aggregatedConstraintSummary.mergeOtherSummary(constraintSummary);
		}
	}

	private void handleTaskLoadStateChange(TaskLoadStateChange msg) {
		this.currentTaskLoads.put(msg.getVertexId(), msg);
	}

	public void enqueueMessage(AbstractSerializableQosMessage message) {
		this.qosMessages.add(message);
	}

	public void shutdown() {
		this.interrupt();
	}
}
