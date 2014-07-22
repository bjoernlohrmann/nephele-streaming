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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.taskmanager.profiling.TaskInfo;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosStatistic;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosValue;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReporterConfigCenter;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class TaskChain {

	private static final Logger LOG = Logger.getLogger(TaskChain.class);

	private final AtomicBoolean taskControlFlowsUnderManipulation = new AtomicBoolean(
			false);

	private final ArrayList<TaskInfo> tasksInChain = new ArrayList<TaskInfo>();

	private final QosStatistic aggregateCpuUtilization = new QosStatistic(
			TaskInfo.CPU_STATISTIC_WINDOW_SIZE);

	public TaskChain(TaskInfo task) {
		this.tasksInChain.add(task);
		fixTasksChainedStatus();
	}

	/**
	 * For internal use only.
	 */
	protected TaskChain() {
	}

	private void fixTasksChainedStatus() {
		if (this.tasksInChain.size() == 1) {
			this.tasksInChain.get(0).setIsChained(false);
			this.tasksInChain.get(0).setNextInChain(null);
		} else {
			for (int i = 0; i < this.tasksInChain.size(); i++) {
				TaskInfo task = this.tasksInChain.get(i);
				task.setIsChained(true);
				if (i < this.tasksInChain.size() - 1) {
					task.setNextInChain(this.tasksInChain.get(i + 1));
				}
			}
		}
	}

	public void measureCPUUtilizationIfPossible() {
		if (this.taskControlFlowsUnderManipulation.get()) {
			return;
		}

		double aggregateUtilization = 0;
		for (TaskInfo task : this.tasksInChain) {
			task.measureCpuUtilization();
			aggregateUtilization += task.getCPUUtilization();
		}

		this.aggregateCpuUtilization.addValue(new QosValue(
				aggregateUtilization, System.currentTimeMillis()));
	}

	public boolean hasCPUUtilizationMeasurements() {
		return this.aggregateCpuUtilization.hasValues();
	}

	public double getCPUUtilization() {
		return this.aggregateCpuUtilization.getArithmeticMean();
	}

	public int getNumberOfChainedTasks() {
		return this.tasksInChain.size();
	}

	public TaskInfo getTask(int index) {
		return this.tasksInChain.get(index);
	}

	public TaskInfo getFirstTask() {
		return this.tasksInChain.get(0);
	}

	public TaskInfo getLastTask() {
		return this.tasksInChain.get(this.tasksInChain.size() - 1);
	}

	public JobID getJobID() {
		return getFirstTask().getTask().getJobID();
	}

	public static Pair<TaskChain, TaskChain> splitAndAnnounceChain(
			TaskChain chain, int splitIndex, ExecutorService backgroundWorkers,
			final QosReporterConfigCenter configCenter) {

		final TaskChain newLeftChain = new TaskChain();
		final TaskChain newRightChain = new TaskChain();

		newLeftChain.tasksInChain.addAll(chain.tasksInChain.subList(0,
				splitIndex));
		newRightChain.tasksInChain.addAll(chain.tasksInChain.subList(
				splitIndex, chain.tasksInChain.size()));

		newLeftChain.fixTasksChainedStatus();
		newRightChain.fixTasksChainedStatus();

		newLeftChain.taskControlFlowsUnderManipulation.set(true);
		newRightChain.taskControlFlowsUnderManipulation.set(true);

		backgroundWorkers.execute(new Runnable() {
			@Override
			public void run() {
				try {
					ChainingUtil.unchainAndAnnounceTaskThreads(newLeftChain,
							newRightChain, configCenter);

				} catch (Exception e) {
					LOG.error("Error during chain construction.", e);
				} finally {
					newLeftChain.taskControlFlowsUnderManipulation.set(false);
					newRightChain.taskControlFlowsUnderManipulation.set(false);

				}
			}
		});

		return Pair.of(newLeftChain, newRightChain);
	}

	public static TaskChain mergeAndAnnounceChains(List<TaskChain> subchains,
			ExecutorService backgroundWorkers,
			final QosReporterConfigCenter configCenter) {

		final TaskChain mergedChain = new TaskChain();

		for (TaskChain subchain : subchains) {
			mergedChain.tasksInChain.addAll(subchain.tasksInChain);
		}

		mergedChain.fixTasksChainedStatus();
		mergedChain.taskControlFlowsUnderManipulation.set(true);

		backgroundWorkers.execute(new Runnable() {
			@Override
			public void run() {
				try {
					ChainingUtil.chainTaskThreads(mergedChain, configCenter);
				} catch (Exception e) {
					LOG.error("Error during chain construction.", e);
				} finally {
					mergedChain.taskControlFlowsUnderManipulation.set(false);
				}
			}
		});

		return mergedChain;
	}

	@Override
	public String toString() {
		StringBuilder toReturn = new StringBuilder();
		toReturn.append(this.tasksInChain.get(0).getTask().getEnvironment()
				.getTaskName());
		toReturn.append(this.tasksInChain.get(0).getTask().getEnvironment()
				.getIndexInSubtaskGroup());

		for (int i = 1; i < this.tasksInChain.size(); i++) {
			TaskInfo nextToChain = this.tasksInChain.get(i);
			toReturn.append("->");
			toReturn.append(nextToChain.getTask().getEnvironment()
					.getTaskName());
			toReturn.append(nextToChain.getTask().getEnvironment()
					.getIndexInSubtaskGroup());
		}

		return toReturn.toString();
	}

}
