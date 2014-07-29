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

package eu.stratosphere.nephele.streaming.jobmanager;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.InternalJobStatus;
import eu.stratosphere.nephele.executiongraph.JobStatusListener;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.plugins.JobManagerPlugin;
import eu.stratosphere.nephele.plugins.PluginID;
import eu.stratosphere.nephele.profiling.ProfilingListener;
import eu.stratosphere.nephele.streaming.ConstraintUtil;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.message.AbstractSerializableQosMessage;

/**
 * Job manager plugin that analyzes the constraints attached to a Nephele job
 * and sets up Qos reporting and management.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class StreamJobManagerPlugin implements JobManagerPlugin,
		JobStatusListener {

	private static final Log LOG = LogFactory
			.getLog(StreamJobManagerPlugin.class);

	private final PluginID pluginID;


	private ConcurrentHashMap<JobID, QosSetupManager> qosSetupManagers = new ConcurrentHashMap<JobID, QosSetupManager>();

	public StreamJobManagerPlugin(final PluginID pluginID) {
		this.pluginID = pluginID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobGraph rewriteJobGraph(final JobGraph jobGraph) {
		try {
			List<JobGraphLatencyConstraint> constraints = ConstraintUtil
					.getConstraints(jobGraph.getJobConfiguration());

			if (!constraints.isEmpty()) {
				QosSetupManager qosSetupManager = new QosSetupManager(
						jobGraph.getJobID(), constraints);
				this.qosSetupManagers.put(jobGraph.getJobID(), qosSetupManager);

				qosSetupManager.rewriteJobGraph(jobGraph);
			}
		} catch (IOException e) {
			LOG.error(
					"Error when deserializing stream latency constraint(s). Not setting up any constraints.",
					e);
		}

		return jobGraph;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ExecutionGraph rewriteExecutionGraph(
			final ExecutionGraph executionGraph) {

		JobID jobId = executionGraph.getJobID();
		if (this.qosSetupManagers.containsKey(jobId)) {
			this.qosSetupManagers.get(jobId).registerOnExecutionGraph(
					executionGraph);
		}

		return executionGraph;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void shutdown() {
		for (QosSetupManager qosSetupManager : this.qosSetupManagers.values()) {
			qosSetupManager.shutdown();
		}
		this.qosSetupManagers.clear();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void sendData(final IOReadableWritable data) throws IOException {
		if (data instanceof AbstractSerializableQosMessage) {
			AbstractSerializableQosMessage qosMessage = (AbstractSerializableQosMessage) data;

			QosSetupManager qosSetupManager = this.qosSetupManagers
					.get(qosMessage.getJobID());
			if (qosSetupManager != null) {
				qosSetupManager.handleMessage(qosMessage);
			}
		} else {
			LOG.error("Job manger streaming plugin received unexpected data of type "
					+ data);

		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IOReadableWritable requestData(final IOReadableWritable data)
			throws IOException {
		LOG.error("Job manger streaming plugin received unexpected data request of type "
				+ data);
		return null;
	}

	@Override
	public void jobStatusHasChanged(ExecutionGraph executionGraph,
			InternalJobStatus newJobStatus, String optionalMessage) {

		if (newJobStatus == InternalJobStatus.FAILED
				|| newJobStatus == InternalJobStatus.CANCELED
				|| newJobStatus == InternalJobStatus.FINISHED) {

			QosSetupManager qosSetupManager = this.qosSetupManagers
					.remove(executionGraph.getJobID());

			if (qosSetupManager != null) {
				qosSetupManager.shutdown();
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean requiresProfiling() {
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ProfilingListener getProfilingListener(final JobID jobID) {
		return null;
	}

	public PluginID getPluginID() {
		return this.pluginID;
	}
}
