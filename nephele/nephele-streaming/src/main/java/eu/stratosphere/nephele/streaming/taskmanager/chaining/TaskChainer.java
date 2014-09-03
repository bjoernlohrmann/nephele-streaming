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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.streaming.message.action.CandidateChainConfig;
import eu.stratosphere.nephele.streaming.taskmanager.profiling.TaskInfo;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReporterConfigCenter;

/**
 * @author Bjoern Lohrmann
 */
public class TaskChainer {
	private final static Logger LOG = Logger.getLogger(TaskChainer.class);

	/**
	 * Holds a list adjacent chains. The order in this list corresponds to their
	 * actual order during execution. Each chain is one thread, plus an
	 * undefined number of user threads created by the user code in the tasks.
	 */
	private final ArrayList<TaskChain> chains = new ArrayList<TaskChain>();

	private final ArrayList<TaskInfo> chainingCandidates;

	private final ExecutorService backgroundWorkers;

	private final QosReporterConfigCenter configCenter;

	public TaskChainer(List<TaskInfo> chainingCandidates,
			ExecutorService backgroundWorkers,
			QosReporterConfigCenter configCenter) {

		this.chainingCandidates = new ArrayList<TaskInfo>(chainingCandidates);
		this.backgroundWorkers = backgroundWorkers;
		this.configCenter = configCenter;

		for (TaskInfo task : chainingCandidates) {
			this.chains.add(new TaskChain(task));
		}
	}

	public void attemptDynamicChaining() {
		if (!areChainsReadyForDynamicChaining()) {
			return;
		}

		if (LOG.isDebugEnabled()) {
			dumpChains();
		}

		mergeChainsIfPossible();
		splitChainsIfNecessary();
	}

	private void dumpChains() {
		StringBuilder message = new StringBuilder();
		message.append("Task chaining attemp with chains: \n");

		for (int i = 0; i < this.chains.size(); i++) {
			TaskChain chain = this.chains.get(i);
			double unchainedUtilizations[] = new double[chain.getNumberOfChainedTasks()];
			for (int j = 0; j < chain.getNumberOfChainedTasks(); j++) {
				unchainedUtilizations[j] = chain.getTask(j).getUnchainedCpuUtilization();
			}

			message.append(i).append(": ").append(this.chains.get(i))
					.append(" (cpu = ")
					.append(this.chains.get(i).getCPUUtilization()).append(", unchained cpu = ")
					.append(Arrays.toString(unchainedUtilizations))
					.append(").\n");
		}
		LOG.debug(message);
	}

	/**
	 * Searches for chains that continually use up more than one CPU core and
	 * splits all of them up into two chains in the background.
	 */
	private void splitChainsIfNecessary() {

		int currChainIndex = 0;
		while (currChainIndex < this.chains.size()) {
			TaskChain chain = this.chains.get(currChainIndex);

			if (chain.hasCPUUtilizationMeasurements()
					&& chain.getCPUUtilization() > 99
					&& chain.getNumberOfChainedTasks() > 1) {

				Pair<TaskChain, TaskChain> splitResult = splitChain(chain);
				this.chains.set(currChainIndex, splitResult.getLeft());
				this.chains.add(currChainIndex + 1, splitResult.getRight());
				currChainIndex = currChainIndex + 2;
			} else {
				currChainIndex++;
			}
		}
	}

	/**
	 * Searches for the highest number of chains that can be merged into a
	 * single chain without using up more than one CPU core (actually 91% of a
	 * core to allow for fluctuations) merges them in the background.
	 */
	private boolean mergeChainsIfPossible() {
		// index of chain where to start the merge (inclusive)
		int bestMergeStart = -1;

		// index of chain where to end the merge (exclusive)
		int bestMergeEnd = 0;

		double bestMergeCpuUtil = 0;

		for (int currMergeStart = 0; currMergeStart < this.chains.size() - 1; currMergeStart++) {

			double currMergeCpuUtil = 0;

			int currMergeEnd = currMergeStart;

			while (currMergeEnd < this.chains.size()) {
				double newCpuUtil = this.chains.get(currMergeEnd)
						.getCPUUtilization();
				if (currMergeCpuUtil + newCpuUtil <= 91.0) {
					currMergeCpuUtil += newCpuUtil;
				} else {
					break;
				}
				currMergeEnd++;
			}

			if (currMergeEnd - currMergeStart > 1
					&& currMergeCpuUtil > bestMergeCpuUtil) {
				bestMergeStart = currMergeStart;
				bestMergeEnd = currMergeEnd;
				bestMergeCpuUtil = currMergeCpuUtil;
			}
		}

		if (bestMergeStart != -1) {
			mergeChains(bestMergeStart, bestMergeEnd);
			return true;
		}

		return false;
	}

	/**
	 * Splits up the chain at the given index into two chains with the goal of
	 * having one remainder chain that never uses more than one CPU core and
	 * another remainder chain that uses as little CPU as possible. The idea
	 * behind this is for the latter remainder chain to be split up later on (if
	 * it is uses a lot of CPU) or be merged with another chain (if it uses
	 * little CPU) at a later point.
	 */
	private Pair<TaskChain, TaskChain> splitChain(TaskChain flow) {

		// pair contains: <noOfTasksToUnchain, estimatedRemainingCPUUtilization>
		Pair<Integer, Double> beginUnchainProposal = computeUnchainingProposal(
				flow, true);
		Pair<Integer, Double> endUnchainProposal = computeUnchainingProposal(
				flow, false);

		// The remainder CPU util x is 0%<x<99% in both unchaining proposals. We
		// will choose the proposal with the higher x.
		int splitIndex;
		if (beginUnchainProposal.getRight() >= endUnchainProposal.getRight()) {
			splitIndex = beginUnchainProposal.getLeft();
		} else {
			splitIndex = flow.getNumberOfChainedTasks()
					- endUnchainProposal.getLeft();
		}

		return TaskChain.splitAndAnnounceChain(flow, splitIndex,
				this.backgroundWorkers, this.configCenter);
	}

	private Pair<Integer, Double> computeUnchainingProposal(TaskChain chain,
			boolean unchainAtBeginning) {

		int noOfChainedTasks = chain.getNumberOfChainedTasks();
		double estimatedRemainingCpuUtil = chain.getCPUUtilization();

		int noOfTasksToUnchain = 0;
		while (estimatedRemainingCpuUtil >= 99
				&& noOfTasksToUnchain < noOfChainedTasks - 1) {

			noOfTasksToUnchain++;

			TaskInfo task;
			if (unchainAtBeginning) {
				task = chain.getTask(noOfTasksToUnchain - 1);
			} else {
				task = chain.getTask(noOfChainedTasks - noOfTasksToUnchain);
			}
			estimatedRemainingCpuUtil -= task.getUnchainedCpuUtilization();
		}

		return Pair.of(noOfTasksToUnchain, estimatedRemainingCpuUtil);
	}

	private void mergeChains(int mergeStart, int mergeEnd) {
		List<TaskChain> chainsToMerge = this.chains.subList(mergeStart,
				mergeEnd);

		TaskChain mergedChain = TaskChain.mergeAndAnnounceChains(chainsToMerge,
				this.backgroundWorkers, this.configCenter);

		this.chains.removeAll(chainsToMerge);
		this.chains.add(mergeStart, mergedChain);
	}

	private boolean areChainsReadyForDynamicChaining() {
		for (TaskChain flow : this.chains) {
			if (!flow.hasCPUUtilizationMeasurements()) {
				return false;
			}

			if (!flow.allTasksAreInRunningState()) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Reset (unchain) each edge and announce chain status.
	 */
	public void resetAndAnnounceAllChaines() throws InterruptedException {
		ChainingUtil.announceAllTasksUnchained(chains, configCenter);

		this.chains.clear();
		for (TaskInfo task : chainingCandidates) {
			this.chains.add(new TaskChain(task));
		}
	}

	public boolean containsTask(ExecutionVertexID vertexID) {
		for (TaskInfo task : this.chainingCandidates) {
			if (task.getVertexID().equals(vertexID))
				return true;
		}

		return false;
	}

	public CandidateChainConfig toCandidateChainConfig() {
		LinkedList<ExecutionVertexID> verticies = new LinkedList<ExecutionVertexID>();

		for (TaskInfo task : this.chainingCandidates)
			verticies.add(task.getVertexID());

		return new CandidateChainConfig(verticies);
	}
}
