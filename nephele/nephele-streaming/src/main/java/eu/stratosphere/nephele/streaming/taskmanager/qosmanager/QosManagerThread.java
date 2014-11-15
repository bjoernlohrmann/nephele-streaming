package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.message.AbstractQosMessage;
import eu.stratosphere.nephele.streaming.message.ChainUpdates;
import eu.stratosphere.nephele.streaming.message.QosManagerConstraintSummaries;
import eu.stratosphere.nephele.streaming.message.action.DeployInstanceQosManagerRoleAction;
import eu.stratosphere.nephele.streaming.message.action.DeployInstanceQosRolesAction;
import eu.stratosphere.nephele.streaming.message.action.QosManagerConfig;
import eu.stratosphere.nephele.streaming.message.qosreport.QosReport;
import eu.stratosphere.nephele.streaming.taskmanager.StreamMessagingThread;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.OutputBufferLatencyManager;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosManagerID;
import eu.stratosphere.nephele.streaming.util.StreamPluginConfig;

/**
 * Implements a thread that serves as a Qos manager. It is started by invoking
 * {@link Thread#start()} and can by shut down with {@link #shutdown()}. It
 * continuously processes {@link AbstractStreamMessage} objects from a
 * threadsafe queue and triggers Qos actions if necessary.
 * {@link #handOffStreamingData(AbstractStreamMessage)} can be used to enqueue
 * data.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class QosManagerThread extends Thread {

	private static final Log LOG = LogFactory.getLog(QosManagerThread.class);
	
	public final static long WAIT_BEFORE_FIRST_ADJUSTMENT = 10 * 1000;

	private JobID jobID;

	private final LinkedBlockingQueue<AbstractQosMessage> streamingDataQueue;

	private OutputBufferLatencyManager oblManager;

	private QosModel qosModel;
	
	private HashMap<LatencyConstraintID, QosLogger> qosLoggers;

	private InstanceConnectionInfo jmConnectionInfo;

	private QosManagerID qosManagerID;

	private final long adjustmentInterval;

	private long timeOfNextAdjustment;
	
	private class MessageStats {
		
		int noOfMessages = 0;
		int noOfEdgeLatencies = 0;
		int noOfVertexLatencies = 0;
		int noOfEdgeStatistics = 0;
		int noOfVertexAnnounces = 0;
		int noOfEdgeAnnounces = 0;
		
		public void logAndReset(QosModel.State state) {
			LOG.debug(String.format("total messages: %d (edge: %d lats and %d stats | vertex: %d | edgeReporters: %d | vertexReporters: %d) || enqueued: %d || QosModel: %s",
							noOfMessages, noOfEdgeLatencies,
							noOfEdgeStatistics, noOfVertexLatencies,
							noOfEdgeAnnounces, noOfVertexAnnounces,
							streamingDataQueue.size(),
							state.toString()));

			noOfMessages = 0;
			noOfEdgeLatencies = 0;
			noOfVertexLatencies = 0;
			noOfEdgeStatistics = 0;
			noOfEdgeAnnounces = 0;
			noOfVertexAnnounces = 0;
		}

		public void updateWithReport(QosReport qosReport) {
			noOfEdgeLatencies += qosReport.getEdgeLatencies().size();
			noOfVertexLatencies += qosReport.getVertexStatistics()
					.size();
			noOfEdgeStatistics += qosReport.getEdgeStatistics().size();
			noOfVertexAnnounces += qosReport
					.getVertexQosReporterAnnouncements().size();
			noOfEdgeAnnounces += qosReport
					.getEdgeQosReporterAnnouncements().size();
		}
	}

	public QosManagerThread(JobID jobID) {
		this.jobID = jobID;
		
		this.adjustmentInterval = StreamPluginConfig.getAdjustmentIntervalMillis();

		this.timeOfNextAdjustment = QosUtils.alignToInterval(
				System.currentTimeMillis() + WAIT_BEFORE_FIRST_ADJUSTMENT,
				this.adjustmentInterval);
		
		this.qosModel = new QosModel(jobID);
		this.streamingDataQueue = new LinkedBlockingQueue<AbstractQosMessage>();
		this.oblManager = new OutputBufferLatencyManager(jobID);
		this.qosLoggers = new HashMap<LatencyConstraintID, QosLogger>();
		this.setName(String.format("QosManagerThread (JobID: %s)",
				jobID.toString()));

		// Determine interface address that is announced to the job manager
		String jmHost = GlobalConfiguration.getString(
				ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
		int jmIpcPort = GlobalConfiguration.getInteger(
				ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
				ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

		try {
			this.jmConnectionInfo = new InstanceConnectionInfo(
					InetAddress.getByName(jmHost), jmIpcPort, jmIpcPort);
		} catch (UnknownHostException e) {
			LOG.error("Error when resolving job manager hostname", e);
		}
	}

	@Override
	public void run() {
		LOG.info("Started Qos manager thread.");

		MessageStats stats = new MessageStats();

		try {
			while (!interrupted()) {
				AbstractQosMessage streamingData = this.streamingDataQueue
						.poll(50, TimeUnit.MILLISECONDS);

				if (streamingData != null) {
					stats.noOfMessages++;
					processStreamingData(stats, streamingData);
				}
				
				adjustIfNecessary(stats);
			}

		} catch (InterruptedException e) {
			// do nothing
		} catch (Exception e) {
			LOG.error("Error in QosManager thread", e);
		} finally {
			this.cleanUp();
		}

		LOG.info("Stopped Qos Manager thread");
	}

	private void adjustIfNecessary(MessageStats stats)
			throws InterruptedException {

		long beginAdjustTime = System.currentTimeMillis();
		if (!isAdjustmentNecessary(beginAdjustTime)) {
			return;
		}

		List<QosConstraintSummary> constraintSummaries = null;
		
		if (this.qosModel.isReady()) {
			QosConstraintViolationListener listener = this.oblManager
					.getQosConstraintViolationListener();

			constraintSummaries = this.qosModel.findQosConstraintViolationsAndSummarize(listener);

			this.oblManager.applyAndSendBufferAdjustments(beginAdjustTime);

		}
		
		if(constraintSummaries == null) {
			constraintSummaries = createEmptyConstraintSummaries();
		}
		
		sendConstraintSummariesToJm(constraintSummaries, beginAdjustTime);
		if (JobManager.getInstance() == null) {
			logConstraintSummaries(constraintSummaries);
		}

		long now = System.currentTimeMillis();
		stats.logAndReset(qosModel.getState());
		this.refreshTimeOfNextAdjustment(now);
	}
	
	private List<QosConstraintSummary> createEmptyConstraintSummaries() {
		List<QosConstraintSummary> emptySummaries = new LinkedList<QosConstraintSummary>();
		
		for(JobGraphLatencyConstraint constraint : qosModel.getJobGraphLatencyConstraints()) {		
			emptySummaries.add(new QosConstraintSummary(constraint, new QosConstraintViolationReport(constraint)));
		}
		
		return emptySummaries;
	}

	private boolean isAdjustmentNecessary(long now) {
		return now >= this.timeOfNextAdjustment;
	}

	private void processStreamingData(MessageStats stats,
			AbstractQosMessage streamingData) {
		
		if (streamingData instanceof QosReport) {
			QosReport qosReport = (QosReport) streamingData;
			this.qosModel.processQosReport(qosReport);
			stats.updateWithReport(qosReport);
		} else if (streamingData instanceof DeployInstanceQosRolesAction) {
			throw new RuntimeException("Got unexpected DeployInstanceQosRolesAction@QosManager!");
		} else if (streamingData instanceof DeployInstanceQosManagerRoleAction) {
			QosManagerConfig config = ((DeployInstanceQosManagerRoleAction) streamingData).getQosManager();
			this.qosManagerID = config.getQosManagerID();
			this.qosModel.mergeShallowQosGraph(config.getShallowQosGraph());
		} else if (streamingData instanceof ChainUpdates) {
			this.qosModel.processChainUpdates((ChainUpdates) streamingData);
		}
	}
	
	private void refreshTimeOfNextAdjustment(long now) {
		while (this.timeOfNextAdjustment <= now) {
			this.timeOfNextAdjustment += this.adjustmentInterval;
		}
	}

	private void sendConstraintSummariesToJm(
			List<QosConstraintSummary> constraintSummaries, long timestamp)
			throws InterruptedException {
		
		if(qosManagerID != null) {
			StreamMessagingThread.getInstance().sendAsynchronously(
					this.jmConnectionInfo,
					new QosManagerConstraintSummaries(jobID, qosManagerID, timestamp, constraintSummaries));
			
		} else {
			LOG.warn("Cannot send constraint summaries because QosManagerID is unknown");
		}
	}

	private void logConstraintSummaries(
			List<QosConstraintSummary> constraintSummaries) {

		for (QosConstraintSummary constraintSummary : constraintSummaries) {
			logConstraintSummary(constraintSummary);
		}
	}

	private void logConstraintSummary(QosConstraintSummary constraintSummary) {
		LatencyConstraintID constraintID = constraintSummary
				.getLatencyConstraintID();

		QosLogger logger = this.qosLoggers.get(constraintID);

		try {
			if (logger == null) {
				logger = new QosLogger(qosModel.getJobGraphLatencyConstraint(constraintID), adjustmentInterval);
				this.qosLoggers.put(constraintID, logger);
			}
			logger.logSummary(constraintSummary);
		} catch (IOException e) {
			LOG.error("Exception in QosLogger", e);
		}
	}

	private void cleanUp() {
		this.streamingDataQueue.clear();
		this.qosModel = null;
		this.oblManager = null;
	}

	public void shutdown() {
		this.interrupt();
	}

	public void handOffStreamingData(AbstractQosMessage data) {
		this.streamingDataQueue.add(data);
	}
}
