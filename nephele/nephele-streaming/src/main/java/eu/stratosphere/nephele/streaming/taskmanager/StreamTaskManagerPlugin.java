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

package eu.stratosphere.nephele.streaming.taskmanager;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.plugins.PluginManager;
import eu.stratosphere.nephele.plugins.TaskManagerPlugin;
import eu.stratosphere.nephele.profiling.ProfilingException;
import eu.stratosphere.nephele.streaming.message.AbstractQosMessage;
import eu.stratosphere.nephele.streaming.message.action.DestroyInstanceQosRolesAction;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.StreamJobEnvironment;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.StreamTaskEnvironment;
import eu.stratosphere.nephele.taskmanager.Task;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Task manager plugin that implements Qos reporting and management.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class StreamTaskManagerPlugin implements TaskManagerPlugin {

	/**
	 * The log object.
	 */
	private static final Log LOG = LogFactory
			.getLog(StreamTaskManagerPlugin.class);

	/**
	 * Provides access to the configuration entry which defines the interval in
	 * which records shall be tagged.
	 */
	public static final String SAMPLING_PROBABILITY_KEY = PluginManager
			.prefixWithPluginNamespace("streaming.qosreporter.samplingprobability");

	/**
	 * The default sampling probability in percent.
	 */
	public static final int DEFAULT_SAMPLING_PROBABILITY = 10;

	/**
	 * Provides access to the configuration entry which defines the interval in
	 * which received tags shall be aggregated and sent to the job manager
	 * plugin component.
	 */
	public static final String AGGREGATION_INTERVAL_KEY = PluginManager
			.prefixWithPluginNamespace("streaming.qosreporter.aggregationinterval");

	/**
	 * The default aggregation interval.
	 */
	private static final long DEFAULT_AGGREGATION_INTERVAL = 1000;

	/**
	 * Stores the instance of the streaming task manager plugin.
	 */
	private static volatile StreamTaskManagerPlugin INSTANCE = null;

	private final ConcurrentMap<JobID, StreamJobEnvironment> streamJobEnvironments = new ConcurrentHashMap<JobID, StreamJobEnvironment>();

	/**
	 * The sampling probability as specified in the plugin configuration.
	 */
	private final int defaultSamplingProbability;

	/**
	 * The aggregation interval as specified in the plugin configuration.
	 */
	private final long defaultAggregationInterval;

	public StreamTaskManagerPlugin() {
		this.defaultSamplingProbability = GlobalConfiguration.getInteger(
				SAMPLING_PROBABILITY_KEY, DEFAULT_SAMPLING_PROBABILITY);
		this.defaultAggregationInterval = GlobalConfiguration.getLong(
				AGGREGATION_INTERVAL_KEY, DEFAULT_AGGREGATION_INTERVAL);

		LOG.info(String
				.format("Configured tagging interval is every %d records / Aggregation interval is %d millis ",
						this.defaultSamplingProbability,
						this.defaultAggregationInterval));

		INSTANCE = this;
	}

	public static StreamTaskManagerPlugin getInstance() {
		if (INSTANCE == null) {
			throw new IllegalStateException(
					"StreamingTaskManagerPlugin has not been initialized");
		}
		return INSTANCE;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void shutdown() {
		StreamMessagingThread.destroyInstance();

		for (StreamJobEnvironment jobEnvironment : this.streamJobEnvironments
				.values()) {

			jobEnvironment.shutdownEnvironment();

		}
		this.streamJobEnvironments.clear();

		INSTANCE = null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerTask(final Task task,
			final Configuration jobConfiguration,
			final IOReadableWritable pluginData) {

		try {
			if (task instanceof RuntimeTask) {

				RuntimeEnvironment runtimeEnv = (RuntimeEnvironment) task
						.getEnvironment();
				if (runtimeEnv.getInvokable().getEnvironment() instanceof StreamTaskEnvironment) {
					StreamTaskEnvironment streamEnv = (StreamTaskEnvironment) runtimeEnv
							.getInvokable().getEnvironment();

					// unfortunately, Nephele's runtime environment does not
					// know its ExecutionVertexID.
					streamEnv.setVertexID(task.getVertexID());
					this.getOrCreateJobEnvironment(runtimeEnv.getJobID())
							.registerTask((RuntimeTask) task, streamEnv);
				}

				// process attached plugin data, such as Qos manager/reporter
				// configs
				if (pluginData != null) {
					try {
						this.sendData(pluginData);
					} catch (IOException e) {
						LOG.error("Error when consuming attached plugin data",
								e);
					}
				}
			}
		} catch (Exception e) {
			LOG.error(e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void unregisterTask(final Task task) {
		try {
			if (task instanceof RuntimeTask) {
				this.getOrCreateJobEnvironment(task.getJobID()).unregisterTask(
						task.getVertexID(),
						((RuntimeTask) task).getRuntimeEnvironment());
			}
		} catch (Exception e) {
			LOG.error(e);
		}
	}

	private StreamJobEnvironment getOrCreateJobEnvironment(JobID jobID)
			throws ProfilingException {

		StreamJobEnvironment jobEnvironment = this.streamJobEnvironments
				.get(jobID);

		if (jobEnvironment == null) {
			jobEnvironment = this.createJobEnvironmentIfNecessary(jobID);
		}

		return jobEnvironment;
	}

	private StreamJobEnvironment createJobEnvironmentIfNecessary(JobID jobID)
			throws ProfilingException {

		StreamJobEnvironment jobEnvironment;
		synchronized (this.streamJobEnvironments) {
			// test again to avoid race conditions
			if (this.streamJobEnvironments.containsKey(jobID)) {
				jobEnvironment = this.streamJobEnvironments.get(jobID);
			} else {
				jobEnvironment = new StreamJobEnvironment(jobID);
				this.streamJobEnvironments.put(jobID, jobEnvironment);
			}
		}
		return jobEnvironment;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void sendData(final IOReadableWritable data) throws IOException {
		try {
			if (data instanceof DestroyInstanceQosRolesAction) {
				JobID jobID = ((DestroyInstanceQosRolesAction) data).getJobID();
				StreamJobEnvironment jobEnv = this.streamJobEnvironments.remove(jobID);

				if (jobEnv != null)
					jobEnv.shutdownEnvironment();

			} else if (data instanceof AbstractQosMessage) {
				AbstractQosMessage streamMsg = (AbstractQosMessage) data;
				this.getOrCreateJobEnvironment(streamMsg.getJobID())
						.handleStreamMessage(streamMsg);
			}
		} catch (Exception e) {
			LOG.error(e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IOReadableWritable requestData(final IOReadableWritable data)
			throws IOException {

		return null;
	}

	/**
	 * 
	 * @return The default aggregation interval configured in the streaming
	 *         plugin's configuration.
	 */
	public static long getDefaultAggregationInterval() {
		return getInstance().defaultAggregationInterval;
	}

	/**
	 * 
	 * @return The default sampling probability configured in the streaming plugin's
	 *         configuration.
	 */
	public static int getDefaultSamplingProbability() {
		return getInstance().defaultSamplingProbability;
	}

}
