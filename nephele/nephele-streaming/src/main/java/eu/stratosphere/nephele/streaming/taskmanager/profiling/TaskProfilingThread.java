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
import eu.stratosphere.nephele.streaming.message.TaskLoadStateChange;
import eu.stratosphere.nephele.streaming.message.TaskLoadStateChange.LoadState;
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

	private HashMap<ExecutionVertexID, LoadState> taskLoadStates = new HashMap<ExecutionVertexID, LoadState>();

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
			LoadState lastLoadState = taskLoadStates
					.get(taskInfo.getVertexID());
			LoadState currLoadState = getLoadState(taskInfo);

			if (lastLoadState != currLoadState) {
				taskLoadStates.put(taskInfo.getVertexID(), currLoadState);
				StreamMessagingThread.getInstance()
						.sendAsynchronously(
								jmConnectionInfo,
								new TaskLoadStateChange(taskInfo.getTask()
										.getJobID(), currLoadState, taskInfo
										.getVertexID(), taskInfo
										.getCPUUtilization()));
			}
		}

	}

	private LoadState getLoadState(TaskInfo taskInfo) {
		if(!taskInfo.hasCPUUtilizationMeasurements()) {
			return  null;
		}
		
		double cpuUtilization = taskInfo.getCPUUtilization();
		if (cpuUtilization <= 60) {
			return LoadState.LOW;
		} else if (cpuUtilization > 60 && cpuUtilization <= 85) {
			return LoadState.MEDIUM;
		} else {
			return LoadState.HIGH;
		}
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
