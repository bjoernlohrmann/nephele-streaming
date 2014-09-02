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
package eu.stratosphere.nephele.streaming.taskmanager.chaining;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.profiling.ProfilingException;
import eu.stratosphere.nephele.streaming.message.action.CandidateChainConfig;
import eu.stratosphere.nephele.streaming.taskmanager.profiling.TaskInfo;
import eu.stratosphere.nephele.streaming.taskmanager.profiling.TaskProfilingThread;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReporterConfigCenter;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class ChainManagerThread extends Thread {

	private final static Logger LOG = Logger
			.getLogger(ChainManagerThread.class);

	private final ExecutorService backgroundChainingWorkers = Executors
			.newCachedThreadPool();

	private final CopyOnWriteArraySet<CandidateChainConfig> pendingCandidateChainConfigs = new CopyOnWriteArraySet<CandidateChainConfig>();

	private final CopyOnWriteArraySet<ExecutionVertexID> pendingUnregisteredTasks = new CopyOnWriteArraySet<ExecutionVertexID>();

	private final ArrayList<TaskChainer> taskChainers = new ArrayList<TaskChainer>();

	private final QosReporterConfigCenter configCenter;

	private boolean started;

	public ChainManagerThread(QosReporterConfigCenter configCenter)
			throws ProfilingException {
		
		this.configCenter = configCenter;
		this.started = false;
		this.setName("ChainManager");
	}

	@Override
	public void run() {
		LOG.info("ChainManager thread started");
		int counter = 0;
		try {
			while (!interrupted()) {
				this.processPendingUnregisteredTasks();
				this.processPendingCandidateChainConfigs();

				if (counter == 5) {
					this.attemptDynamicChaining();
					counter = 0;
				}

				counter++;
				Thread.sleep(1000);
			}
		} catch (InterruptedException e) {

		} finally {
			cleanUp();
			LOG.info("ChainManager thread stopped.");
		}
	}

	private void attemptDynamicChaining() {
		for (TaskChainer taskChainer : this.taskChainers) {
			taskChainer.attemptDynamicChaining();
		}
	}

	private void processPendingCandidateChainConfigs() {
		for (CandidateChainConfig candidateChainConfig : this.pendingCandidateChainConfigs) {
			boolean success = tryToCreateTaskChainer(candidateChainConfig);
			if (success) {
				this.pendingCandidateChainConfigs.remove(candidateChainConfig);
			}
		}
	}

	private boolean tryToCreateTaskChainer(
			CandidateChainConfig candidateChainConfig) {

		ArrayList<TaskInfo> taskInfos = new ArrayList<TaskInfo>();
		for (ExecutionVertexID vertexID : candidateChainConfig
				.getChainingCandidates()) {

			TaskInfo taskInfo = TaskProfilingThread.getTaskInfo(vertexID);
			if (taskInfo == null) {
				return false;
			}

			taskInfos.add(taskInfo);
		}

		this.taskChainers.add(new TaskChainer(taskInfos,
				this.backgroundChainingWorkers, this.configCenter));

		return true;
	}

	/**
	 * Find and invalidate chainers containing unregistered (scaled down) tasks.
	 * 
	 * Removes all paths containg unregistered tassks and announce them as unchained.
	 */
	private void processPendingUnregisteredTasks() throws InterruptedException {
		HashSet<TaskChainer> chainsToRemove = new HashSet<TaskChainer>();

		for (ExecutionVertexID vertexID : this.pendingUnregisteredTasks) {
			for (TaskChainer taskChainer : this.taskChainers) {
				// the whole path becomes invalid if it contains one
				// unregistered task
				if (taskChainer.containsTask(vertexID)) {
					chainsToRemove.add(taskChainer);
				}
			}

			this.pendingUnregisteredTasks.remove(vertexID);
		}

		for (TaskChainer taskChainer : chainsToRemove) {
			this.taskChainers.remove(taskChainer);
			this.pendingCandidateChainConfigs.add(taskChainer.toCandidateChainConfig());
			taskChainer.resetAndAnnounceAllChaines();
		}

		if (chainsToRemove.size() > 0)
			LOG.debug("Invalidating " + chainsToRemove.size() + " chains: "
					+ "pendingConfigs=" + this.pendingCandidateChainConfigs.size()
					+ ", pendingUnregister=" + this.pendingUnregisteredTasks.size()
					+ ", active chains=" + this.taskChainers.size());
	}

	private void cleanUp() {
		// FIXME clean up data structures here
	}

	public void shutdown() {
		this.interrupt();
	}

	public void registerCandidateChain(CandidateChainConfig chainConfig) {
		this.pendingCandidateChainConfigs.add(chainConfig);
		
		if (!this.started) {
			this.started = true;
			this.start();
		}

	}

	public void unregisterTask(ExecutionVertexID vertexID) {
		this.pendingUnregisteredTasks.add(vertexID);
	}
}
