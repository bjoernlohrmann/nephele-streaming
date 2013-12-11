/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.nephele.streaming.taskmanager.qosreporter;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.profiling.ProfilingException;
import eu.stratosphere.nephele.streaming.message.AbstractQosMessage;
import eu.stratosphere.nephele.streaming.message.ChainUpdates;
import eu.stratosphere.nephele.streaming.message.action.CandidateChainConfig;
import eu.stratosphere.nephele.streaming.message.action.DeployInstanceQosRolesAction;
import eu.stratosphere.nephele.streaming.message.action.LimitBufferSizeAction;
import eu.stratosphere.nephele.streaming.message.qosreport.QosReport;
import eu.stratosphere.nephele.streaming.taskmanager.StreamTaskManagerPlugin;
import eu.stratosphere.nephele.streaming.taskmanager.chaining.ChainManagerThread;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosManagerThread;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.StreamTaskEnvironment;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;

/**
 * This class implements the Qos management and reporting for the vertices and
 * edges of a specific job on a task manager, while the job is running.
 * 
 * This class is thread-safe.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class StreamJobEnvironment {

	private static final Log LOG = LogFactory
			.getLog(StreamJobEnvironment.class);

	private final JobID jobID;

	private final QosReportForwarderThread qosReportForwarder;

	/**
	 * A special thread that chains/unchains "mapper" tasks (special Nephele
	 * execution vertices that declare themselves as chainable).
	 */
	private final ChainManagerThread chainManager;

	private final HashMap<ExecutionVertexID, StreamTaskQosCoordinator> taskQosCoordinators;

	private volatile boolean environmentIsShutDown;

	private volatile QosManagerThread qosManager;

	public StreamJobEnvironment(JobID jobID) throws ProfilingException {

		this.jobID = jobID;
		this.environmentIsShutDown = false;

		QosReporterConfigCenter reporterConfig = new QosReporterConfigCenter();
		reporterConfig.setAggregationInterval(StreamTaskManagerPlugin
				.getDefaultAggregationInterval());
		reporterConfig.setTaggingInterval(StreamTaskManagerPlugin
				.getDefaultTaggingInterval());

		this.qosReportForwarder = new QosReportForwarderThread(jobID,
				reporterConfig);
		this.chainManager = new ChainManagerThread(reporterConfig);
		this.taskQosCoordinators = new HashMap<ExecutionVertexID, StreamTaskQosCoordinator>();
	}

	public JobID getJobID() {
		return this.jobID;
	}

	public synchronized void registerTask(RuntimeTask task,
			StreamTaskEnvironment streamEnv) {

		if (this.environmentIsShutDown) {
			return;
		}

		this.updateAggregationAndTaggingIntervals(streamEnv);

		synchronized (this.taskQosCoordinators) {
			if (this.taskQosCoordinators.containsKey(task.getVertexID())) {
				throw new RuntimeException(String.format(
						"Task %s is already registered",
						streamEnv.getTaskName()));
			}

			this.taskQosCoordinators.put(task.getVertexID(),
					new StreamTaskQosCoordinator(task, streamEnv,
							this.qosReportForwarder));
		}

		if (streamEnv.isMapperTask()) {
			this.chainManager.registerMapperTask(task);
		}
	}

	private void updateAggregationAndTaggingIntervals(
			Environment taskEnvironment) {
		long aggregationInterval = taskEnvironment
				.getJobConfiguration()
				.getLong(StreamTaskManagerPlugin.AGGREGATION_INTERVAL_KEY,
						StreamTaskManagerPlugin.getDefaultAggregationInterval());
		int taggingInterval = taskEnvironment.getJobConfiguration().getInteger(
				StreamTaskManagerPlugin.TAGGING_INTERVAL_KEY,
				StreamTaskManagerPlugin.getDefaultTaggingInterval());

		this.qosReportForwarder.getConfigCenter().setAggregationInterval(
				aggregationInterval);
		this.qosReportForwarder.getConfigCenter().setTaggingInterval(
				taggingInterval);
	}

	public synchronized void shutdownEnvironment() {
		if (this.environmentIsShutDown) {
			return;
		}

		this.environmentIsShutDown = true;

		for (StreamTaskQosCoordinator qosCoordinator : this.taskQosCoordinators
				.values()) {
			// shuts down Qos reporting for this vertex
			qosCoordinator.shutdownReporting();
		}
		this.taskQosCoordinators.clear();

		if (this.qosManager != null) {
			this.qosManager.shutdown();
		}
		this.qosManager = null;
		this.qosReportForwarder.shutdown();
		this.chainManager.shutdown();
	}

	public void handleStreamMessage(AbstractQosMessage streamMsg) {
		if (this.environmentIsShutDown) {
			return;
		}

		if (streamMsg instanceof QosReport) {
			this.handleQosReport((QosReport) streamMsg);
		} else if (streamMsg instanceof ChainUpdates) {
			this.handleChainUpdates((ChainUpdates) streamMsg);
		} else if (streamMsg instanceof LimitBufferSizeAction) {
			this.handleLimitBufferSizeAction((LimitBufferSizeAction) streamMsg);
		} else if (streamMsg instanceof DeployInstanceQosRolesAction) {
			this.handleDeployInstanceQosRolesAction((DeployInstanceQosRolesAction) streamMsg);
		} else {
			LOG.error("Received message is of unknown type "
					+ streamMsg.getClass());
		}
	}

	private void handleChainUpdates(ChainUpdates chainUpdates) {
		this.ensureQosManagerIsRunning();
		this.qosManager.handOffStreamingData(chainUpdates);
	}

	private void handleDeployInstanceQosRolesAction(
			DeployInstanceQosRolesAction deployRolesAction) {

		if (deployRolesAction.getQosManager() != null) {
			this.processQosManagerConfig(deployRolesAction);
		}

		this.qosReportForwarder.configureReporting(deployRolesAction);

		for (CandidateChainConfig chainConfig : deployRolesAction
				.getCandidateChains()) {
			this.chainManager.registerCandidateChain(chainConfig);
		}

		LOG.info(String
				.format("Deployed %d vertex Qos reporters, %d edge Qos reporters and %d Qos manager roles",
						deployRolesAction.getVertexQosReporters().size(),
						deployRolesAction.getEdgeQosReporters().size(),
						deployRolesAction.getQosManager() != null ? 1 : 0));
	}

	private void handleQosReport(QosReport data) {
		this.ensureQosManagerIsRunning();
		this.qosManager.handOffStreamingData(data);
	}

	private void processQosManagerConfig(
			DeployInstanceQosRolesAction deployRolesAction) {

		this.ensureQosManagerIsRunning();
		this.qosManager.handOffStreamingData(deployRolesAction);
	}

	private void ensureQosManagerIsRunning() {
		// this may seem like clunky code, however
		// this is a highly used code path. Reading a volatile
		// variable is fairly cheap, whereas obtaining an object
		// monitor (as done when calling a synchronized method) is not.
		if (this.qosManager == null) {
			ensureQosManagerIsRunningSynchronized();
		}
	}

	private synchronized void ensureQosManagerIsRunningSynchronized() {
		if (this.qosManager == null) {
			this.qosManager = new QosManagerThread(this.jobID);
			this.qosManager.start();
		}
	}

	private void handleLimitBufferSizeAction(LimitBufferSizeAction action) {

		StreamTaskQosCoordinator qosCoordinator = this.taskQosCoordinators
				.get(action.getVertexID());

		if (qosCoordinator != null) {
			qosCoordinator.handleLimitBufferSizeAction(action);
		}
	}

	@SuppressWarnings("unused")
	public synchronized void unregisterTask(ExecutionVertexID vertexID,
			Environment environment) {

		if (this.environmentIsShutDown) {
			return;
		}

		StreamTaskQosCoordinator qosCoordinator = this.taskQosCoordinators
				.get(vertexID);

		if (qosCoordinator != null) {
			// shuts down Qos reporting for this vertex
			qosCoordinator.shutdownReporting();
			this.taskQosCoordinators.remove(vertexID);

			this.chainManager.unregisterMapperTask(vertexID);
		}

		if (this.taskQosCoordinators.isEmpty()) {
			shutdownEnvironment();
		}
	}
}
