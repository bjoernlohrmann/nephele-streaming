package eu.stratosphere.nephele.streaming.jobmanager.autoscaling;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.message.AbstractQosMessage;
import eu.stratosphere.nephele.streaming.message.AbstractSerializableQosMessage;
import eu.stratosphere.nephele.streaming.message.QosManagerConstraintSummaries;
import eu.stratosphere.nephele.streaming.message.TaskLoadStateChange;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosLogger;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.OutputBufferLatencyManager;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.QosConstraintSummary;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosManagerID;

public class ElasticTaskQosAutoScalingThread extends Thread {

	private static final Log LOG = LogFactory
			.getLog(ElasticTaskQosAutoScalingThread.class);

	private final LinkedBlockingQueue<AbstractSerializableQosMessage> qosMessages = new LinkedBlockingQueue<AbstractSerializableQosMessage>();

	private final HashMap<LatencyConstraintID, JobGraphLatencyConstraint> qosConstraints = new HashMap<LatencyConstraintID, JobGraphLatencyConstraint>();

	private final HashMap<ExecutionVertexID, TaskLoadStateChange> currentTaskLoads = new HashMap<ExecutionVertexID, TaskLoadStateChange>();

	private long timeOfLastScaling;

	private long timeOfNextScaling;
	
	private final HashMap<LatencyConstraintID, QosConstraintSummaryAggregator> aggregators = new HashMap<LatencyConstraintID, QosConstraintSummaryAggregator>();

	private QosLogger logger;
	
	public ElasticTaskQosAutoScalingThread(
			HashMap<LatencyConstraintID, QosGraph> qosGraphs, Set<QosManagerID> qosManagers) {

		this.setName("QosAutoScalingThread");
		this.timeOfLastScaling = 0;
		this.timeOfNextScaling = 0;
		
		for (LatencyConstraintID constraintID : qosGraphs.keySet()) {
			JobGraphLatencyConstraint constraint = qosGraphs.get(constraintID)
					.getConstraintByID(constraintID);

			qosConstraints.put(constraintID, constraint);
			aggregators.put(constraintID, new QosConstraintSummaryAggregator(
					constraint, qosManagers));
		}
		
		try {
			logger = new QosLogger(qosConstraints.values().iterator().next(),
						GlobalConfiguration.getLong(OutputBufferLatencyManager.QOSMANAGER_ADJUSTMENTINTERVAL_KEY,
								OutputBufferLatencyManager.DEFAULT_ADJUSTMENTINTERVAL));
		} catch (IOException e) {
			LOG.error("Exception in QosLogger", e);
		} 
		
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
											OutputBufferLatencyManager.QOSMANAGER_ADJUSTMENTINTERVAL_KEY,
											OutputBufferLatencyManager.DEFAULT_ADJUSTMENTINTERVAL);
				}
			}
		} catch (InterruptedException e) {
			// do nothing
		} catch (Exception e) {
			LOG.error("Exception in auto scaling thread", e);
		} finally {
			cleanUp();
		}

		LOG.info("Qos Auto Scaling Thread stopped.");
	}

	private void doAutoscale() {
		
		HashMap<JobVertexID, Integer> scalingActions = new HashMap<JobVertexID, Integer>();
		
		for(QosConstraintSummaryAggregator summaryAggregator : aggregators.values()) {
			collectScalingActionsForConstraint(
					summaryAggregator.getConstraint(),
					summaryAggregator.computeAggregation(),
					scalingActions);
			
		}		
	}

	private void collectScalingActionsForConstraint(
			JobGraphLatencyConstraint constraint,
			QosConstraintSummary constraintSummary,
			HashMap<JobVertexID, Integer> scalingActions) {
		
		if (logger != null) {
			try {
				logger.logSummary(constraintSummary);
			} catch (IOException e) {
				LOG.error("Error during QoS logging", e);
			}
		}

		// FIXME implement autoscaling algorithm
		// determine load state on constrained subgraph (LOW, MEDIUM or HIGH)
		// if load state is LOW:
		//   execute scale down policy
		// else if load state is HIGH:
		//   execute scale up policy		
	}

	private void cleanUp() {
		// clear large memory structures
		this.qosConstraints.clear();
		this.aggregators.clear();
		this.currentTaskLoads.clear();
	}

	private boolean scalingIsDue(long now) {
		if (now < timeOfNextScaling) {
			return false;
		}

		for (LatencyConstraintID constraintID : qosConstraints.keySet()) {
			QosConstraintSummaryAggregator summaryAggregator = aggregators.get(constraintID);

			if (!summaryAggregator.canAggregate()) {
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

			aggregators.get(constraintID).add(nextMessage.getQosManagerID(),
					constraintSummary);
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
