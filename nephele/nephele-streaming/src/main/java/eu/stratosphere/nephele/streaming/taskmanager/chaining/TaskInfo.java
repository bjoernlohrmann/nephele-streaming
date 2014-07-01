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

import java.lang.management.ThreadMXBean;

import eu.stratosphere.nephele.execution.ExecutionListener;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.profiling.impl.EnvironmentThreadSet;
import eu.stratosphere.nephele.profiling.impl.types.InternalExecutionVertexThreadProfilingData;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosStatistic;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosValue;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.StreamTaskEnvironment;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;

/**
 * @author Bjoern Lohrmann
 * 
 */
public class TaskInfo implements ExecutionListener {

	public static final int CPU_STATISTIC_WINDOW_SIZE = 5;

	private final RuntimeTask task;

	private final ThreadMXBean tmx;

	private QosStatistic cpuUtilization;

	private double unchainedCpuUtilization;

	private volatile EnvironmentThreadSet environmentThreadSet;

	private boolean isChained;

	private TaskInfo nextInChain;

	public TaskInfo(RuntimeTask task, ThreadMXBean tmx) {
		this.task = task;
		this.tmx = tmx;
		this.cpuUtilization = new QosStatistic(CPU_STATISTIC_WINDOW_SIZE);
		this.unchainedCpuUtilization = -1;
		this.task.registerExecutionListener(this);
		this.isChained = false;
		this.nextInChain = null;
	}

	/**
	 * @return the runtime task
	 */
	public RuntimeTask getTask() {
		return this.task;
	}

	public ExecutionVertexID getVertexID() {
		return this.task.getVertexID();
	}

	public StreamTaskEnvironment getStreamTaskEnvironment() {
		RuntimeEnvironment runtimeEnv = (RuntimeEnvironment) getTask()
				.getEnvironment();
		return (StreamTaskEnvironment) runtimeEnv.getInvokable()
				.getEnvironment();
	}

	/**
	 * Returns the environmentThreadSet.
	 * 
	 * @return the environmentThreadSet
	 */
	public EnvironmentThreadSet getEnvironmentThreadSet() {
		return this.environmentThreadSet;
	}

	/**
	 * Sets the environmentThreadSet to the specified value.
	 * 
	 * @param environmentThreadSet
	 *            the environmentThreadSet to set
	 */
	public void setEnvironmentThreadSet(
			EnvironmentThreadSet environmentThreadSet) {

		this.environmentThreadSet = environmentThreadSet;
	}

	public ExecutionState getExecutionState() {
		return this.task.getExecutionState();
	}

	public void measureCpuUtilization() {

		if (this.environmentThreadSet != null) {

			long now = System.currentTimeMillis();
			InternalExecutionVertexThreadProfilingData profilingData = this.environmentThreadSet
					.captureCPUUtilization(this.task.getJobID(), this.tmx, now);

			/**
			 * cpuUtilization measures in percent, how much of one CPU core's
			 * available time the the vertex's main thread AND its associated
			 * user threads have consumed. Example values:
			 * 
			 * cpuUtilization==50 => it uses half a core's worth of CPU time
			 * 
			 * cpuUtilization==200 => it uses two core's worth of CPU time (this
			 * can happen if the vertex's main thread spawns several user
			 * threads)
			 */
			double cpuUtilization = (profilingData.getUserTime()
					+ profilingData.getSystemTime() + profilingData
						.getBlockedTime())
					* (this.environmentThreadSet.getNumberOfUserThreads() + 1);

			this.cpuUtilization.addValue(new QosValue(cpuUtilization, now));
		}
	}

	public boolean hasCPUUtilizationMeasurements() {
		return this.cpuUtilization.hasValues();
	}

	public double getCPUUtilization() {
		return this.cpuUtilization.getArithmeticMean();
	}

	public double getUnchainedCpuUtilization() {
		return this.unchainedCpuUtilization;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.execution.ExecutionListener#executionStateChanged
	 * (eu.stratosphere.nephele.jobgraph.JobID,
	 * eu.stratosphere.nephele.executiongraph.ExecutionVertexID,
	 * eu.stratosphere.nephele.execution.ExecutionState, java.lang.String)
	 */
	@Override
	public void executionStateChanged(JobID jobID, ExecutionVertexID vertexID,
			ExecutionState newExecutionState, String optionalMessage) {

		switch (newExecutionState) {
		case RUNNING:
			this.setEnvironmentThreadSet(new EnvironmentThreadSet(this.tmx,
					getTask().getRuntimeEnvironment().getExecutingThread(),
					vertexID));
			break;
		case FINISHING:
		case FINISHED:
		case CANCELING:
		case CANCELED:
		case FAILED:
		case SUSPENDING:
		case SUSPENDED:
			setEnvironmentThreadSet(null);
			break;
		default:
			break;
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.execution.ExecutionListener#userThreadStarted
	 * (eu.stratosphere.nephele.jobgraph.JobID,
	 * eu.stratosphere.nephele.executiongraph.ExecutionVertexID,
	 * java.lang.Thread)
	 */
	@Override
	public void userThreadStarted(JobID jobID, ExecutionVertexID vertexID,
			Thread userThread) {

		if (this.environmentThreadSet != null) {
			// threadsafe operation
			this.environmentThreadSet.addUserThread(this.tmx, userThread);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.execution.ExecutionListener#userThreadFinished
	 * (eu.stratosphere.nephele.jobgraph.JobID,
	 * eu.stratosphere.nephele.executiongraph.ExecutionVertexID,
	 * java.lang.Thread)
	 */
	@Override
	public void userThreadFinished(JobID jobID, ExecutionVertexID vertexID,
			Thread userThread) {

		if (this.environmentThreadSet != null) {
			// threadsafe operation
			this.environmentThreadSet.removeUserThread(userThread);
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.nephele.execution.ExecutionListener#getPriority()
	 */
	@Override
	public int getPriority() {
		return 2;
	}

	public void cleanUp() {
		this.task.unregisterExecutionListener(this);
	}

	public void setIsChained(boolean isChained) {
		if (!this.isChained && this.hasCPUUtilizationMeasurements()) {
			this.unchainedCpuUtilization = this.getCPUUtilization();
		}

		if (this.hasCPUUtilizationMeasurements()) {
			this.cpuUtilization = new QosStatistic(CPU_STATISTIC_WINDOW_SIZE);
		}

		this.isChained = isChained;
	}

	public void setNextInChain(TaskInfo nextInChain) {
		this.nextInChain = nextInChain;
	}

	public TaskInfo getNextInChain() {
		return this.nextInChain;
	}

	public boolean isChained() {
		return this.isChained;
	}
}
