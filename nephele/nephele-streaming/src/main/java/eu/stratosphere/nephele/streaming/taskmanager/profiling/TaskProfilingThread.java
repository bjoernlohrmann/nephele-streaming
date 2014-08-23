package eu.stratosphere.nephele.streaming.taskmanager.profiling;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.profiling.ProfilingException;
import eu.stratosphere.nephele.streaming.message.CpuLoadClassifier;
import eu.stratosphere.nephele.streaming.message.TaskCpuLoadChange;
import eu.stratosphere.nephele.streaming.taskmanager.StreamMessagingThread;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;

/**
 * Thread that collects CPU profiling data on registered tasks.
 * 
 * This class is thread-safe.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class TaskProfilingThread extends Thread {

	private final static Logger LOG = Logger
			.getLogger(TaskProfilingThread.class);

	private static final ThreadMXBean tmx = ManagementFactory.getThreadMXBean();

	private static final ConcurrentHashMap<ExecutionVertexID, TaskInfo> tasks = new ConcurrentHashMap<ExecutionVertexID, TaskInfo>();

	private static TaskProfilingThread singletonInstance = null;

	private final HashMap<ExecutionVertexID, Double> taskCpuUtilizations = new HashMap<ExecutionVertexID, Double>();

	private InstanceConnectionInfo jmConnectionInfo;

	private TaskProfilingThread() throws ProfilingException,
			UnknownHostException {
		// Initialize MX interface and check if thread contention monitoring is
		// supported
		if (tmx.isThreadContentionMonitoringSupported()) {
			tmx.setThreadContentionMonitoringEnabled(true);
		} else {
			throw new ProfilingException(
					"The thread contention monitoring is not supported.");
		}
		this.setName("TaskProfiler");

		// Determine interface address that is announced to the job manager
		String jmHost = GlobalConfiguration.getString(
				ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
		int jmIpcPort = GlobalConfiguration.getInteger(
				ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
				ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

		this.jmConnectionInfo = new InstanceConnectionInfo(
				InetAddress.getByName(jmHost), jmIpcPort, jmIpcPort);
	}

	@Override
	public void run() {
		LOG.info("TaskProfiler thread started");
		try {
			while (!interrupted()) {
				this.collectThreadProfilingData();
				this.notifyJobManagerOfLoadStateChanges();
				Thread.sleep(1000);
			}
		} catch (InterruptedException e) {

		} finally {
			cleanUp();
			LOG.info("ChainManager thread stopped.");
		}
	}

	private void notifyJobManagerOfLoadStateChanges()
			throws InterruptedException {
		
		for (TaskInfo taskInfo : tasks.values()) {
			Double lastNotified = taskCpuUtilizations.get(taskInfo
					.getVertexID());
			Double curr = getCpuUtilization(taskInfo);

			boolean mustNotify = (lastNotified == null && curr != null)
					|| (lastNotified != null 
					     && curr != null 
					     && ((Math.abs(lastNotified - curr) > 5) 
					    		 || CpuLoadClassifier.fromCpuUtilization(lastNotified) != 
					    		    CpuLoadClassifier.fromCpuUtilization(curr)));

			if (mustNotify) {
				taskCpuUtilizations.put(taskInfo.getVertexID(), curr);
				StreamMessagingThread.getInstance().sendAsynchronously(
						jmConnectionInfo,
						new TaskCpuLoadChange(taskInfo.getTask().getJobID(),
								taskInfo.getVertexID(), taskInfo
										.getCPUUtilization()));
			}
		}

	}

	private Double getCpuUtilization(TaskInfo taskInfo) {
		if(!taskInfo.hasCPUUtilizationMeasurements()) {
			return  null;
		}
		
		return taskInfo.getCPUUtilization();
	}

	private void cleanUp() {
		// FIXME clean up data structures here
	}

	private void collectThreadProfilingData() {
		for (TaskInfo taskInfo : tasks.values()) {
			taskInfo.measureCpuUtilization();
		}
	}

	public static synchronized void shutdown() {
		if (singletonInstance != null) {
			singletonInstance.interrupt();
			singletonInstance = null;
		}
	}

	private static void ensureProfilingThreadIsRunning()
			throws ProfilingException, UnknownHostException {
		if (singletonInstance == null) {
			singletonInstance = new TaskProfilingThread();
			singletonInstance.start();
		}
	}

	public static synchronized void registerTask(RuntimeTask task)
			throws ProfilingException, IOException {

		TaskInfo taskInfo = new TaskInfo(task, tmx);
		tasks.put(task.getVertexID(), taskInfo);

		ensureProfilingThreadIsRunning();
	}

	public static synchronized void unregisterTask(ExecutionVertexID vertexID) {
		TaskInfo taskInfo = tasks.remove(vertexID);
		if (taskInfo != null) {
			taskInfo.cleanUp();
		}

		if (tasks.isEmpty()) {
			shutdown();
		}
	}

	public static TaskInfo getTaskInfo(ExecutionVertexID vertexId) {
		return tasks.get(vertexId);
	}

}
