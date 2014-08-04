package eu.stratosphere.nephele.streaming.taskmanager.qosreporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.message.action.DeployInstanceQosRolesAction;
import eu.stratosphere.nephele.streaming.message.action.EdgeQosReporterConfig;
import eu.stratosphere.nephele.streaming.message.action.VertexQosReporterConfig;
import eu.stratosphere.nephele.streaming.message.qosreport.AbstractQosReportRecord;
import eu.stratosphere.nephele.streaming.message.qosreport.EdgeLatency;
import eu.stratosphere.nephele.streaming.message.qosreport.EdgeStatistics;
import eu.stratosphere.nephele.streaming.message.qosreport.QosReport;
import eu.stratosphere.nephele.streaming.message.qosreport.VertexStatistics;
import eu.stratosphere.nephele.streaming.taskmanager.StreamMessagingThread;
import eu.stratosphere.nephele.streaming.taskmanager.StreamTaskManagerPlugin;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * This class aggregates and forwards stream QoS report data (latencies,
 * throughput, etc) of the tasks of a single job running within the same
 * manager. Each task manager has one instance of this class per job. Qos report
 * data is pre-aggregated for each QoS manager and shipped in a single message
 * to the Qos manager once every {@link #aggregationInterval}. If no QoS data
 * for a Qos manager has been received, messages will be skipped. This class
 * starts its own thread as soon as there is at least on registered task and can
 * be shut down by invoking {@link #shutdown()}.
 * 
 * This class is threadsafe.
 * 
 * @author Bjoern Lohrmann
 */
public class QosReportForwarderThread extends Thread {

	private static final Log LOG = LogFactory
			.getLog(QosReportForwarderThread.class);

	private final JobID jobID;

	private final StreamMessagingThread messagingThread;

	private final QosReporterConfigCenter reporterConfigCenter;

	/**
	 * All aggregated reports (one per Qos manager) sorted in order of their due
	 * time.
	 */
	private AggregatedReport[] pendingReports;

	private int currentReportIndex;

	private ConcurrentHashMap<InstanceConnectionInfo, AggregatedReport> reportByQosManager;

	private ConcurrentHashMap<QosReporterID, Set<AggregatedReport>> reportsByReporter;

	private ConcurrentHashMap<QosReporterID, Boolean> reporterActivityMap;

	private final LinkedBlockingQueue<AbstractQosReportRecord> pendingReportRecords;

	private volatile boolean isShutDown;

	private boolean threadStarted;

	private InstanceConnectionInfo localhost;

	/**
	 * Aggregates all Qos report data to be sent to exactly one Qos manager.
	 */
	private class AggregatedReport implements Comparable<AggregatedReport> {
		private long dueTime;

		private QosReport report;

		private InstanceConnectionInfo qosManager;

		private long reportingOffset;

		public AggregatedReport(InstanceConnectionInfo qosManager,
				long reportingOffset) {

			this.qosManager = qosManager;
			this.report = new QosReport(QosReportForwarderThread.this.jobID);
			long now = System.currentTimeMillis();
			this.dueTime = now
					- now
					% QosReportForwarderThread.this.reporterConfigCenter
							.getAggregationInterval() + reportingOffset;
		}

		public long getDueTime() {
			return this.dueTime;
		}

		public QosReport getReport() {
			return this.report;
		}

		public InstanceConnectionInfo getQosManager() {
			return this.qosManager;
		}

		public void shiftToNextReportingInterval() {
			long now = System.currentTimeMillis();
			while (this.dueTime <= now) {
				this.dueTime = this.dueTime
						+ QosReportForwarderThread.this.reporterConfigCenter
								.getAggregationInterval();
			}
			this.report = new QosReport(QosReportForwarderThread.this.jobID);
		}

		public long getReportingOffset() {
			return this.reportingOffset;
		}

		public boolean isEmpty() {
			return this.report.isEmpty();
		}

		@Override
		public int compareTo(AggregatedReport o) {
			if (this.getReportingOffset() < o.getReportingOffset()) {
				return -1;
			} else if (this.getReportingOffset() > o.getReportingOffset()) {
				return 1;
			} else {
				return 0;
			}
		}
	}

	public QosReportForwarderThread(JobID jobID,
			QosReporterConfigCenter reporterConfig) {

		this.jobID = jobID;
		this.messagingThread = StreamMessagingThread.getInstance();
		this.reporterConfigCenter = reporterConfig;
		this.pendingReports = new AggregatedReport[0];
		this.currentReportIndex = -1;

		// concurrent maps are necessary here because THIS thread and and
		// another thread registering/unregistering
		// Qos reporter configs may access the maps concurrently
		this.reportByQosManager = new ConcurrentHashMap<InstanceConnectionInfo, AggregatedReport>();
		this.reportsByReporter = new ConcurrentHashMap<QosReporterID, Set<AggregatedReport>>();
		this.reporterActivityMap = new ConcurrentHashMap<QosReporterID, Boolean>();
		this.pendingReportRecords = new LinkedBlockingQueue<AbstractQosReportRecord>();
		this.isShutDown = false;
		this.threadStarted = false;
		this.setName(String.format("QosReporterForwarderThread (JobID: %s)",
				jobID.toString()));
	}

	@Override
	public void run() {
		try {
			while (!interrupted()) {

				AggregatedReport currentReport = this.getCurrentReport();
				this.processPendingReportRecords();
				this.sleepUntilReportDue(currentReport);
				this.processPendingReportRecords();

				if (!currentReport.isEmpty()) {
					if (this.isLocalReport(currentReport)) {
						this.sendToLocal(currentReport);
					} else {
						this.sendToRemote(currentReport);
					}
				}
				currentReport.shiftToNextReportingInterval();
			}
		} catch (InterruptedException e) {
		} finally {
			this.cleanUp();
		}
	}

	private boolean isLocalReport(AggregatedReport currentReport) {
		return currentReport.getQosManager().equals(this.localhost);
	}

	private void sendToRemote(AggregatedReport currentReport)
			throws InterruptedException {
		this.messagingThread.sendAsynchronously(
				currentReport.getQosManager(), currentReport.getReport());
	}

	/**
	 * @param currentReport
	 */
	private void sendToLocal(AggregatedReport currentReport) {
		try {
			StreamTaskManagerPlugin.getInstance().sendData(
					currentReport.getReport());
		} catch (IOException e) {
			LOG.error(StringUtils.stringifyException(e));
		}
	}

	private void sleepUntilReportDue(AggregatedReport currentReport)
			throws InterruptedException {
		long sleepTime = Math.max(0,
				currentReport.getDueTime() - System.currentTimeMillis());
		if (sleepTime > 0) {
			sleep(sleepTime);
		}
	}

	private AggregatedReport getCurrentReport() {
		AggregatedReport currentReport;
		synchronized (this.pendingReports) {
			this.currentReportIndex = (this.currentReportIndex + 1)
					% this.pendingReports.length;
			currentReport = this.pendingReports[this.currentReportIndex];
		}
		return currentReport;
	}

	private void cleanUp() {
		this.pendingReports = null;

		this.reportByQosManager.clear();
		this.reportByQosManager = null;

		this.reportsByReporter.clear();
		this.reportsByReporter = null;

		this.reporterActivityMap.clear();
		this.reporterActivityMap = null;

		this.pendingReportRecords.clear();

	}

	private ArrayList<AbstractQosReportRecord> tmpRecords = new ArrayList<AbstractQosReportRecord>();

	private void processPendingReportRecords() {
		this.pendingReportRecords.drainTo(this.tmpRecords);
		for (AbstractQosReportRecord record : this.tmpRecords) {
			if (record instanceof EdgeLatency) {
				this.processEdgeLatency((EdgeLatency) record);
			} else if (record instanceof EdgeStatistics) {
				this.processEdgeStatistics((EdgeStatistics) record);
			} else if (record instanceof VertexStatistics) {
				this.processVertexStatistics((VertexStatistics) record);
			} else {
				LOG.error(String.format("Cannot process report record: %s",
						record.getClass().getSimpleName()));
			}
		}
		this.tmpRecords.clear();
	}

	private Set<AggregatedReport> getReports(QosReporterID reporterID) {
		Set<AggregatedReport> toReturn = this.reportsByReporter.get(reporterID);
		if (toReturn == null) {
			return Collections.emptySet();
		}
		return toReturn;
	}

	private void processVertexStatistics(VertexStatistics taskLatency) {

		QosReporterID.Vertex reporterID = taskLatency.getReporterID();

		if (this.reporterActivityMap.get(reporterID) != Boolean.TRUE) {
			this.activateReporter(reporterID);
		}

		Set<AggregatedReport> reports = this.getReports(reporterID);
		for (AggregatedReport report : reports) {
			report.getReport().addVertexStatistics(taskLatency);
		}
	}

	/**
	 * @param reporterID
	 */
	private void activateReporter(QosReporterID.Vertex reporterID) {
		this.reporterActivityMap.put(reporterID, Boolean.TRUE);

		VertexQosReporterConfig reporterConfig = this.reporterConfigCenter
				.getVertexQosReporter(reporterID);

		Set<AggregatedReport> reports = this.getReports(reporterID);
		for (AggregatedReport report : reports) {
			report.getReport().announceVertexQosReporter(reporterConfig);
		}
	}

	private void processEdgeStatistics(EdgeStatistics channelStats) {

		QosReporterID.Edge reporterID = channelStats.getReporterID();

		if (this.reporterActivityMap.get(reporterID) != Boolean.TRUE) {
			this.activateReporter(reporterID);
		}

		Set<AggregatedReport> reports = this.getReports(reporterID);
		for (AggregatedReport report : reports) {
			report.getReport().addEdgeStatistics(channelStats);
		}
	}

	/**
	 * @param reporterID
	 */
	private void activateReporter(QosReporterID.Edge reporterID) {
		this.reporterActivityMap.put(reporterID, Boolean.TRUE);

		EdgeQosReporterConfig reporterConfig = this.reporterConfigCenter
				.getEdgeQosReporter(reporterID);

		Set<AggregatedReport> reports = this.getReports(reporterID);
		for (AggregatedReport report : reports) {
			report.getReport().addEdgeQosReporterAnnouncement(reporterConfig);
		}
	}

	private void processEdgeLatency(EdgeLatency channelLatency) {
		Set<AggregatedReport> reports = this.getReports(channelLatency
				.getReporterID());
		for (AggregatedReport report : reports) {
			report.getReport().addEdgeLatency(channelLatency);
		}
	}

	public void configureReporting(DeployInstanceQosRolesAction rolesDeployment) {

		if (this.isShutDown) {
			return;
		}

		synchronized (this.pendingReports) {
			if (this.localhost == null) {
				this.localhost = rolesDeployment.getInstanceConnectionInfo();
			}

			for (EdgeQosReporterConfig edgeReporter : rolesDeployment
					.getEdgeQosReporters()) {
				this.registerEdgeQosReporter(edgeReporter);
			}

			for (VertexQosReporterConfig vertexReporter : rolesDeployment
					.getVertexQosReporters()) {
				this.registerVertexQosReporter(vertexReporter);
			}

			LOG.debug(String
					.format("Prepared Qos reports for %d vertices and %d edges. Max %d reports each reporting interval.",
							rolesDeployment.getVertexQosReporters().size(),
							rolesDeployment.getEdgeQosReporters().size(),
							this.reportByQosManager.size()));

			if (!this.threadStarted) {
				this.start();
				this.threadStarted = true;
			}
		}
	}

	private void registerEdgeQosReporter(EdgeQosReporterConfig edgeReporter) {

		this.reporterConfigCenter.addEdgeQosReporter(edgeReporter);

		QosReporterID reporterID = edgeReporter.getReporterID();
		Set<AggregatedReport> edgeReports = this
				.getOrCreateReportSet(reporterID);

		for (InstanceConnectionInfo qosManager : edgeReporter.getQosManagers()) {
			edgeReports.add(this.getOrCreateAggregatedReport(qosManager));
		}

		this.reporterActivityMap.put(reporterID, Boolean.FALSE);

		LOG.debug(String.format(
				"Registered Qos reports to %d QosManagers for QosReporter %s",
				edgeReports.size(), edgeReporter.getName()));
	}

	private void registerVertexQosReporter(
			VertexQosReporterConfig vertexReporter) {

		this.reporterConfigCenter.addVertexQosReporter(vertexReporter);

		QosReporterID reporterID = vertexReporter.getReporterID();
		Set<AggregatedReport> vertexReports = this
				.getOrCreateReportSet(reporterID);
		for (InstanceConnectionInfo qosManager : vertexReporter
				.getQosManagers()) {
			vertexReports.add(this.getOrCreateAggregatedReport(qosManager));
		}
		this.reporterActivityMap.put(reporterID, Boolean.FALSE);
	}

	private Set<AggregatedReport> getOrCreateReportSet(QosReporterID reporterID) {
		Set<AggregatedReport> reports = this.reportsByReporter.get(reporterID);

		if (reports == null) {
			// concurrent sets are necessary here because THIS thread and and
			// another thread registering/unregistering
			// reports may access the maps concurrently
			reports = new CopyOnWriteArraySet<AggregatedReport>();
			this.reportsByReporter.put(reporterID, reports);
		}
		return reports;
	}

	private AggregatedReport getOrCreateAggregatedReport(
			InstanceConnectionInfo qosManager) {
		AggregatedReport qosManagerReport = this.reportByQosManager
				.get(qosManager);
		if (qosManagerReport == null) {
			qosManagerReport = this.createAndEnqueueReport(qosManager);
		}
		return qosManagerReport;
	}

	private AggregatedReport createAndEnqueueReport(
			InstanceConnectionInfo qosManager) {

		AggregatedReport newReport = new AggregatedReport(qosManager,
				(long) (Math.random() * this.reporterConfigCenter
						.getAggregationInterval()));
		this.reportByQosManager.put(qosManager, newReport);

		// insert new report in pendingReports, sorted by reporting offset
		AggregatedReport[] newPendingReports = new AggregatedReport[this.pendingReports.length + 1];
		System.arraycopy(this.pendingReports, 0, newPendingReports, 0,
				this.pendingReports.length);
		newPendingReports[newPendingReports.length - 1] = newReport;
		Arrays.sort(newPendingReports);
		this.pendingReports = newPendingReports;

		return newReport;
	}

	public void addToNextReport(AbstractQosReportRecord record) {
		if (this.isShutDown) {
			return;
		}
		this.pendingReportRecords.add(record);
	}

	public void shutdown() {
		this.isShutDown = true;
		this.interrupt();
	}

	public QosReporterConfigCenter getConfigCenter() {
		return this.reporterConfigCenter;
	}
}
