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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.message.ChainUpdates;
import eu.stratosphere.nephele.streaming.message.action.DropCurrentChainAction;
import eu.stratosphere.nephele.streaming.message.action.EdgeQosReporterConfig;
import eu.stratosphere.nephele.streaming.message.action.EstablishNewChainAction;
import eu.stratosphere.nephele.streaming.taskmanager.StreamMessagingThread;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReporterConfigCenter;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.StreamTaskEnvironment;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.chaining.RuntimeChain;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.chaining.RuntimeChainLink;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class ChainingUtil {

	public static void chainTaskThreads(TaskChain chainModel,
			QosReporterConfigCenter configCenter) throws InterruptedException {

		RuntimeChain runtimeChain = assembleRuntimeChain(chainModel);
		runtimeChain.getFirstOutputGate().enqueueQosAction(
				new EstablishNewChainAction(runtimeChain));
		runtimeChain.waitUntilTasksAreChained();

		announceNewChainingStatus(chainModel.getJobID(),
				collectChainedEdges(chainModel), true, configCenter);
	}

	private static void announceNewChainingStatus(JobID jobID,
			Set<QosReporterID.Edge> edges, boolean newlyChained,
			QosReporterConfigCenter configCenter) throws InterruptedException {

		HashMap<InstanceConnectionInfo, ChainUpdates> updatesByQosManager = new HashMap<InstanceConnectionInfo, ChainUpdates>();

		for (QosReporterID.Edge edge : edges) {
			EdgeQosReporterConfig edgeReporterConfig = configCenter
					.getEdgeQosReporter(edge);

			for (InstanceConnectionInfo qosManager : edgeReporterConfig
					.getQosManagers()) {

				ChainUpdates update = updatesByQosManager.get(qosManager);
				if (update == null) {
					update = new ChainUpdates(jobID);
					updatesByQosManager.put(qosManager, update);
				}

				if (newlyChained) {
					update.addNewlyChainedEdge(edge);
				} else {
					update.addUnchainedEdge(edge);
				}
			}
		}

		StreamMessagingThread msgThread = StreamMessagingThread.getInstance();
		for (InstanceConnectionInfo qosManager : updatesByQosManager.keySet()) {
			msgThread.sendAsynchronously(qosManager,
					updatesByQosManager.get(qosManager));
		}
	}

	private static Set<QosReporterID.Edge> collectChainedEdges(
			TaskChain chainModel) {

		HashSet<QosReporterID.Edge> toReturn = new HashSet<QosReporterID.Edge>();

		for (int i = 0; i < chainModel.getNumberOfChainedTasks() - 1; i++) {
			toReturn.add(QosReporterID.forEdge(chainModel.getTask(i)
					.getStreamTaskEnvironment().getOutputGate(0)
					.getOutputChannel(0).getID()));
		}

		return toReturn;
	}

	private static RuntimeChain assembleRuntimeChain(TaskChain chainModel) {
		ArrayList<RuntimeChainLink> chainLinks = new ArrayList<RuntimeChainLink>();
		for (int i = 0; i < chainModel.getNumberOfChainedTasks(); i++) {
			StreamTaskEnvironment taskEnvironment = chainModel.getTask(i)
					.getStreamTaskEnvironment();
			chainLinks.add(new RuntimeChainLink(taskEnvironment, taskEnvironment.getIocTask(),
					taskEnvironment.getInputGate(0), taskEnvironment
					.getOutputGate(0)));
		}

		return new RuntimeChain(chainLinks);
	}

	public static void unchainAndAnnounceTaskThreads(TaskChain leftChain,
																									 TaskChain rightChain, QosReporterConfigCenter configCenter)
			throws InterruptedException {

		if (leftChain.getNumberOfChainedTasks() > 1) {
			RuntimeChain leftRuntimeChain = assembleRuntimeChain(leftChain);
			leftRuntimeChain.getFirstOutputGate().enqueueQosAction(
					new EstablishNewChainAction(leftRuntimeChain));
			leftRuntimeChain.waitUntilTasksAreChained();
		} else {
			leftChain.getTask(0).getStreamTaskEnvironment().getOutputGate(0)
					.enqueueQosAction(new DropCurrentChainAction());
		}

		if (rightChain.getNumberOfChainedTasks() > 1) {
			RuntimeChain rightRuntimeChain = assembleRuntimeChain(rightChain);
			rightRuntimeChain.getFirstOutputGate().enqueueQosAction(
					new EstablishNewChainAction(rightRuntimeChain));
			rightRuntimeChain.getFirstInputGate().wakeUpTaskThreadIfNecessary();
			rightRuntimeChain.waitUntilTasksAreChained();
		} else {
			rightChain.getTask(0).getStreamTaskEnvironment().getInputGate(0)
					.wakeUpTaskThreadIfNecessary();
		}

		QosReporterID.Edge unchainedEdge = QosReporterID.forEdge(leftChain
				.getLastTask().getStreamTaskEnvironment().getOutputGate(0)
				.getOutputChannel(0).getID());

		announceNewChainingStatus(leftChain.getJobID(),
				Collections.singleton(unchainedEdge), false, configCenter);
	}

	/**
	 * Announce all edges in given chains as unchained.
	 */
	public static void announceAllTasksUnchained(Collection<TaskChain> chains,
			QosReporterConfigCenter configCenter) throws InterruptedException {

		HashSet<QosReporterID.Edge> unchainedEdges = new HashSet<QosReporterID.Edge>();
		JobID jobID = null;
		for (TaskChain chain : chains) {
			unchainedEdges.addAll(collectChainedEdges(chain));
			jobID = chain.getJobID();
		}

		announceNewChainingStatus(jobID, unchainedEdges, false, configCenter);
	}
}
