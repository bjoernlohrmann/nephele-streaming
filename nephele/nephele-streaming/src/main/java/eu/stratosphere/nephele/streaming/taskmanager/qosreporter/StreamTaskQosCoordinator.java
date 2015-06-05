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

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.streaming.message.action.EdgeQosReporterConfig;
import eu.stratosphere.nephele.streaming.message.action.LimitBufferSizeAction;
import eu.stratosphere.nephele.streaming.message.action.SetOutputBufferLifetimeTargetAction;
import eu.stratosphere.nephele.streaming.message.action.VertexQosReporterConfig;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.listener.InputGateQosReportingListener;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.listener.OutputGateQosReportingListener;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex.VertexStatisticsReportManager;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.StreamTaskEnvironment;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamInputGate;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamOutputGate;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;
import eu.stratosphere.nephele.types.Record;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Set;

/**
 * An instance of this class implements Qos data reporting for a specific vertex
 * and its ingoing/outgoing edges on a task manager while the vertex actually
 * runs.
 *
 * This class is thread-safe.
 *
 * @author Bjoern Lohrmann
 *
 */
public class StreamTaskQosCoordinator implements QosReporterConfigListener {

	private static final Log LOG = LogFactory
			.getLog(StreamTaskQosCoordinator.class);

	private final RuntimeTask task;

	private final StreamTaskEnvironment taskEnvironment;

	private final QosReportForwarderThread reporterThread;

	private final QosReporterConfigCenter reporterConfigCenter;

	/**
	 * For each input/output gate combination for which Qos reports are
	 * required, this {@link VertexStatisticsReportManager} creates the reports.
	 */
	private VertexStatisticsReportManager vertexStatisticsManager;

	private boolean isShutdown;

	public StreamTaskQosCoordinator(RuntimeTask task,
			StreamTaskEnvironment taskEnvironment,
			QosReportForwarderThread reportForwarder) {

		this.task = task;
		this.taskEnvironment = taskEnvironment;
		this.reporterThread = reportForwarder;
		this.reporterConfigCenter = reportForwarder.getConfigCenter();

		this.vertexStatisticsManager = new VertexStatisticsReportManager(
				this.reporterThread,
				this.taskEnvironment.getNumberOfInputGates(),
				this.taskEnvironment.getNumberOfOutputGates());
		this.isShutdown = false;

		this.prepareQosReporting();
	}

	private void prepareQosReporting() {
		this.installVertexStatisticsReporters();
		this.installInputGateListeners();
		this.installOutputGateListeners();
	}

	private void installVertexStatisticsReporters() {
		Set<VertexQosReporterConfig> vertexReporterConfigs = this.reporterConfigCenter
				.getVertexQosReporters(this.task.getVertexID());

		if (vertexReporterConfigs.isEmpty()) {
			this.reporterConfigCenter.setQosReporterConfigListener(
					this.task.getVertexID(), this);
		} else {
			for (VertexQosReporterConfig reporterConfig : vertexReporterConfigs) {
				this.installVertexStatisticsReporter(reporterConfig);
			}
		}
	}

	private void installVertexStatisticsReporter(
			VertexQosReporterConfig reporterConfig) {

		QosReporterID.Vertex reporterID = reporterConfig.getReporterID();

		if (this.vertexStatisticsManager.containsReporter(reporterID)) {
			return;
		}

		int inputGateIndex = -1;
		if (reporterConfig.getInputGateID() != null) {
			StreamInputGate<? extends Record> inputGate = this.taskEnvironment
					.getInputGate(reporterConfig.getInputGateID());
			ensureInputGateListener(inputGate);
		}

		int outputGateIndex = -1;
		if (reporterConfig.getOutputGateID() != null) {
			StreamOutputGate<? extends Record> outputGate = this.taskEnvironment
					.getOutputGate(reporterConfig.getOutputGateID());
			ensureOutputGateListener(outputGate);
		}

		this.vertexStatisticsManager.addReporter(inputGateIndex,
				outputGateIndex, reporterID, reporterConfig.getSamplingStrategy());

	}

	private void installInputGateListeners() {
		for (int i = 0; i < this.taskEnvironment.getNumberOfInputGates(); i++) {
			StreamInputGate<? extends Record> inputGate = this.taskEnvironment
					.getInputGate(i);

			// as constraints are defined on job graph level, it is safe to only
			// test one channel
			boolean mustReportQosForGate = this.reporterConfigCenter
					.getEdgeQosReporter(inputGate.getInputChannel(0)
							.getConnectedChannelID()) != null;

			if (!mustReportQosForGate) {
				this.reporterConfigCenter.setQosReporterConfigListener(
						inputGate.getGateID(), this);
				break;
			}

			for (int j = 0; j < inputGate.getNumberOfInputChannels(); j++) {
				int runtimeChannelIndex = inputGate.getInputChannel(j)
						.getChannelIndex();
				ChannelID sourceChannelID = inputGate.getInputChannel(j)
						.getConnectedChannelID();

				EdgeQosReporterConfig edgeReporter = this.reporterConfigCenter
						.getEdgeQosReporter(sourceChannelID);
				QosReporterID.Edge reporterID = (QosReporterID.Edge) edgeReporter
						.getReporterID();
				vertexStatisticsManager.addInputGateReporter(inputGate.getIndex(), runtimeChannelIndex,
						inputGate.getNumberOfInputChannels(), reporterID);
			}

			ensureInputGateListener(inputGate);
		}
	}

	private void installOutputGateListeners() {
		for (int i = 0; i < this.taskEnvironment.getNumberOfOutputGates(); i++) {
			StreamOutputGate<? extends Record> outputGate = this.taskEnvironment
					.getOutputGate(i);

			// as constraints are defined on job graph level, it is safe to only
			// test one channel
			boolean mustReportQosForGate = this.reporterConfigCenter
					.getEdgeQosReporter(outputGate.getOutputChannel(0).getID()) != null;

			if (!mustReportQosForGate) {
				this.reporterConfigCenter.setQosReporterConfigListener(
						outputGate.getGateID(), this);
				break;
			}

			for (int j = 0; j < outputGate.getNumberOfOutputChannels(); j++) {
				int runtimeChannelIndex = outputGate.getOutputChannel(j)
						.getChannelIndex();
				ChannelID sourceChannelID = outputGate.getOutputChannel(j)
						.getID();

				EdgeQosReporterConfig edgeReporter = this.reporterConfigCenter
						.getEdgeQosReporter(sourceChannelID);
				QosReporterID.Edge reporterID = (QosReporterID.Edge) edgeReporter
						.getReporterID();
				vertexStatisticsManager.addOutputGateReporter(outputGate.getIndex(), runtimeChannelIndex,
						outputGate.getNumberOfOutputChannels(), reporterID);
			}

			ensureOutputGateListener(outputGate);
		}
	}

	public synchronized void handleLimitBufferSizeAction(
			LimitBufferSizeAction limitBufferSizeAction) {

		if (this.isShutdown) {
			return;
		}

		StreamOutputGate<?> outputGate = this.taskEnvironment
				.getOutputGate(limitBufferSizeAction.getOutputGateID());

		if (outputGate != null) {
			ChannelID sourceChannelID = limitBufferSizeAction
					.getSourceChannelID();
			EdgeQosReporterConfig edgeReporter = this.reporterThread
					.getConfigCenter().getEdgeQosReporter(sourceChannelID);

			if (edgeReporter != null) {
				LOG.debug(String
						.format("Setting buffer size output channel %s (%s) to %d bytes",
								sourceChannelID, edgeReporter.getName(),
								limitBufferSizeAction.getBufferSize()));
				outputGate.enqueueQosAction(limitBufferSizeAction);
			}
		}
	}

	public void handleSetOutputLatencyTargetAction(
			SetOutputBufferLifetimeTargetAction action) {

		if (this.isShutdown) {
			return;
		}

		StreamOutputGate<?> outputGate = this.taskEnvironment
				.getOutputGate(action.getOutputGateID());

		if (outputGate != null) {
			ChannelID sourceChannelID = action
					.getSourceChannelID();
			EdgeQosReporterConfig edgeReporter = this.reporterThread
					.getConfigCenter().getEdgeQosReporter(sourceChannelID);

			if (edgeReporter != null) {
				LOG.debug(String
						.format("Setting obl target for %s (%s) to %d ms",
								sourceChannelID, edgeReporter.getName(),
								action.getOutputBufferLifetimeTarget()));
				outputGate.enqueueQosAction(action);
			}
		}
	}


	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.nephele.streaming.taskmanager.qosreporter.
	 * QosReporterConfigListener
	 * #newVertexQosReporter(eu.stratosphere.nephele.streaming
	 * .message.action.VertexQosReporterConfig)
	 */
	@Override
	public synchronized void newVertexQosReporter(
			VertexQosReporterConfig reporterConfig) {
		if (this.isShutdown) {
			return;
		}

		this.installVertexStatisticsReporter(reporterConfig);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.nephele.streaming.taskmanager.qosreporter.
	 * QosReporterConfigListener
	 * #newEdgeQosReporter(eu.stratosphere.nephele.streaming
	 * .message.action.EdgeQosReporterConfig)
	 */
	@Override
	public synchronized void newEdgeQosReporter(
			EdgeQosReporterConfig edgeReporter) {
		if (this.isShutdown) {
			return;
		}

		QosReporterID.Edge reporterID = (QosReporterID.Edge) edgeReporter
				.getReporterID();

		StreamInputGate<? extends Record> inputGate = this.taskEnvironment
				.getInputGate(edgeReporter.getInputGateID());

		StreamOutputGate<? extends Record> outputGate = this.taskEnvironment
				.getOutputGate(edgeReporter.getOutputGateID());

		if (inputGate != null) {
			int runtimeGateIndex = inputGate.getIndex();
			int runtimeChannelIndex = inputGate.getInputChannel(
					edgeReporter.getTargetChannelID()).getChannelIndex();

			vertexStatisticsManager.addInputGateReporter(runtimeGateIndex,
					runtimeChannelIndex, inputGate.getNumberOfInputChannels(), reporterID);

			ensureInputGateListener(inputGate);
		} else if (outputGate != null) {
			int runtimeGateIndex = outputGate.getIndex();
			int runtimeChannelIndex = outputGate.getOutputChannel(
					edgeReporter.getSourceChannelID()).getChannelIndex();

			vertexStatisticsManager.addOutputGateReporter(runtimeGateIndex,
					runtimeChannelIndex, outputGate.getNumberOfOutputChannels(), reporterID);

			ensureOutputGateListener(outputGate);
		}
	}

	private void ensureInputGateListener(StreamInputGate<? extends Record> inputGate) {
		InputGateQosReportingListener listener = inputGate.getQosReportingListener();
		if (listener == null) {
			listener = new InputGateListener(inputGate.getIndex());
			inputGate.setQosReportingListener(listener);
		}
	}

	private void ensureOutputGateListener(StreamOutputGate<? extends Record> outputGate) {
		OutputGateQosReportingListener listener = outputGate.getQosReportingListener();
		if (listener == null) {
			listener = new OutputGateListener(outputGate.getIndex());
			outputGate.setQosReportingListener(listener);
		}
	}

	public synchronized void shutdownReporting() {
		this.isShutdown = true;
		shutdownInputGateReporters();
		shutdownOutputGateReporters();
		this.vertexStatisticsManager = null;
		this.reporterConfigCenter.unsetQosReporterConfigListener(this.task.getVertexID());
	}

	private void shutdownOutputGateReporters() {
		for (int i = 0; i < this.taskEnvironment.getNumberOfOutputGates(); i++) {
			StreamOutputGate<? extends Record> outputGate = this.taskEnvironment
					.getOutputGate(i);
			outputGate.setQosReportingListener(null);
			this.reporterConfigCenter.unsetQosReporterConfigListener(outputGate.getGateID());
		}
	}

	private void shutdownInputGateReporters() {
		for (int i = 0; i < this.taskEnvironment.getNumberOfInputGates(); i++) {
			StreamInputGate<? extends Record> inputGate = this.taskEnvironment
					.getInputGate(i);
			inputGate.setQosReportingListener(null);
			this.reporterConfigCenter.unsetQosReporterConfigListener(inputGate.getGateID());
		}
	}

	private class OutputGateListener implements OutputGateQosReportingListener {
		private final int gateIndex;

		public OutputGateListener(int gateIndex) {
			this.gateIndex = gateIndex;
		}

		@Override
		public void outputBufferSent(int channelIndex, long currentAmountTransmitted) {
			vertexStatisticsManager.outputBufferSent(gateIndex, channelIndex, currentAmountTransmitted);
		}

		@Override
		public void recordEmitted(int channelIndex, AbstractTaggableRecord record) {
			vertexStatisticsManager.recordEmitted(gateIndex, channelIndex, record);
		}

		@Override
		public void outputBufferAllocated(int channelIndex) {
			vertexStatisticsManager.outputBufferAllocated(gateIndex, channelIndex);
		}
	}

	private class InputGateListener implements InputGateQosReportingListener {
		private final int gateIndex;

		public InputGateListener(int gateIndex) {
			this.gateIndex = gateIndex;
		}

		@Override
		public void recordReceived(int channelIndex, AbstractTaggableRecord record) {
			vertexStatisticsManager.recordReceived(gateIndex);
			TimestampTag timestampTag = (TimestampTag) record.getTag();
			if (timestampTag != null) {
				vertexStatisticsManager.reportLatenciesIfNecessary(gateIndex, channelIndex, timestampTag);
			}
		}

		@Override
		public void tryingToReadRecord() {
			vertexStatisticsManager.tryingToReadRecord(gateIndex);
		}

		@Override
		public void inputBufferConsumed(int channelIndex, long bufferInterarrivalTimeNanos, int recordsReadFromBuffer) {
			vertexStatisticsManager.inputBufferConsumed(gateIndex, channelIndex, bufferInterarrivalTimeNanos,
					recordsReadFromBuffer);
		}
	}
}
