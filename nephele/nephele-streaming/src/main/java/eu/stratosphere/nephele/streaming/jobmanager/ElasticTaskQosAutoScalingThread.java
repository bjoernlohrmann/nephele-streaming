package eu.stratosphere.nephele.streaming.jobmanager;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.message.AbstractQosMessage;
import eu.stratosphere.nephele.streaming.message.TaskLoadStateChange;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;

public class ElasticTaskQosAutoScalingThread extends Thread {

	private static final Log LOG = LogFactory
			.getLog(ElasticTaskQosAutoScalingThread.class);

	private LinkedBlockingQueue<AbstractQosMessage> qosMessages = new LinkedBlockingQueue<AbstractQosMessage>();

	private HashMap<LatencyConstraintID, QosGraph> qosGraphs;

	public ElasticTaskQosAutoScalingThread(
			HashMap<LatencyConstraintID, QosGraph> qosGraphs) {
		this.setName("Qos Auto Scaling Thread started");
		this.qosGraphs = qosGraphs;
		this.start();
	}

	@Override
	public void run() {

		processMessages();

		// FIXME make scaling decision (or not)
	}

	private void processMessages() {
		while (!qosMessages.isEmpty()) {
			AbstractQosMessage nextMessage = qosMessages.poll();
			if (nextMessage instanceof TaskLoadStateChange) {
				handleTaskLoadStateChange((TaskLoadStateChange) nextMessage);
			}
		}
	}

	private void handleTaskLoadStateChange(TaskLoadStateChange nextMessage) {
		// FIXME
	}

	public void enqueueMessage(AbstractQosMessage message) {
		this.qosMessages.add(message);
	}

	public void shutdown() {
		this.interrupt();
	}
}
