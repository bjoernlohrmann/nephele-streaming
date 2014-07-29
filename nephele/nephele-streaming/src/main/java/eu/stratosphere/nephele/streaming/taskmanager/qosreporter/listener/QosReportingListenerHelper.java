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
package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.listener;

import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.InputGateReporterManager;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.OutputGateReporterManager;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.TimestampTag;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.VertexStatisticsReportManager;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamInputGate;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamOutputGate;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;
import eu.stratosphere.nephele.types.Record;

/**
 * Utility class that creates {@link InputGateQosReportingListener}
 * {@link OutputGateQosReportingListener} and adds them to the respective
 * input/output gates.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class QosReportingListenerHelper {

	public static void listenToVertexLatencyOnInputGate(
			StreamInputGate<? extends Record> inputGate,
			final VertexStatisticsReportManager vertexStatsManager) {

		final int gateIndex = inputGate.getIndex();

		InputGateQosReportingListener listener = new InputGateQosReportingListener() {
			@Override
			public void recordReceived(int inputChannelIndex,
					AbstractTaggableRecord record) {
				vertexStatsManager.recordReceived(gateIndex);
			}
		};
		InputGateQosReportingListener oldListener = inputGate
				.getQosReportingListener();
		if (oldListener != null) {
			inputGate.setQosReportingListener(createChainedListener(
					oldListener, listener));
		} else {
			inputGate.setQosReportingListener(listener);
		}
	}

	public static void listenToVertexLatencyOnOutputGate(
			StreamOutputGate<? extends Record> outputGate,
			final VertexStatisticsReportManager vertexStatsManager) {

		final int gateIndex = outputGate.getIndex();

		OutputGateQosReportingListener listener = new OutputGateQosReportingListener() {
			@Override
			public void recordEmitted(int outputChannelIndex,
					AbstractTaggableRecord record) {
				vertexStatsManager.recordEmitted(gateIndex);
			}

			@Override
			public void outputBufferSent(int outputChannelIndex,
					long currentAmountTransmitted) {
				// do nothing
			}
		};

		OutputGateQosReportingListener oldListener = outputGate
				.getQosReportingListener();

		if (oldListener != null) {
			outputGate.setQosReportingListener(createChainedListener(listener,
					oldListener));
		} else {
			outputGate.setQosReportingListener(listener);
		}
	}

	public static void listenToChannelLatenciesOnInputGate(
			StreamInputGate<? extends Record> inputGate,
			final InputGateReporterManager reporter) {

		InputGateQosReportingListener listener = new InputGateQosReportingListener() {
			@Override
			public void recordReceived(int inputChannelIndexinRuntimeGate,
					AbstractTaggableRecord record) {

				TimestampTag timestampTag = (TimestampTag) record.getTag();

				if (timestampTag != null) {
					reporter.reportLatencyIfNecessary(
							inputChannelIndexinRuntimeGate, timestampTag);
				}
			}
		};

		InputGateQosReportingListener oldListener = inputGate
				.getQosReportingListener();

		if (oldListener != null) {
			inputGate.setQosReportingListener(createChainedListener(listener,
					oldListener));
		} else {
			inputGate.setQosReportingListener(listener);
		}
	}

	public static void listenToOutputChannelStatisticsOnOutputGate(
			StreamOutputGate<? extends Record> outputGate,
			final OutputGateReporterManager gateReporterManager) {

		OutputGateQosReportingListener listener = new OutputGateQosReportingListener() {

			@Override
			public void outputBufferSent(int runtimeGateChannelIndex,
					long currentAmountTransmitted) {
				gateReporterManager.outputBufferSent(runtimeGateChannelIndex,
						currentAmountTransmitted);
			}

			@Override
			public void recordEmitted(int runtimeGateChannelIndex,
					AbstractTaggableRecord record) {
				gateReporterManager.recordEmitted(runtimeGateChannelIndex,
						record);
			}
		};

		OutputGateQosReportingListener oldListener = outputGate
				.getQosReportingListener();

		if (oldListener != null) {
			outputGate.setQosReportingListener(createChainedListener(
					oldListener, listener));
		} else {
			outputGate.setQosReportingListener(listener);
		}
	}

	private static InputGateQosReportingListener createChainedListener(
			final InputGateQosReportingListener first,
			final InputGateQosReportingListener second) {

		return new InputGateQosReportingListener() {

			@Override
			public void recordReceived(int inputChannel,
					AbstractTaggableRecord record) {
				first.recordReceived(inputChannel, record);
				second.recordReceived(inputChannel, record);
			}
		};
	}

	private static OutputGateQosReportingListener createChainedListener(
			final OutputGateQosReportingListener first,
			final OutputGateQosReportingListener second) {

		return new OutputGateQosReportingListener() {

			@Override
			public void outputBufferSent(int channelIndex,
					long currentAmountTransmitted) {
				first.outputBufferSent(channelIndex, currentAmountTransmitted);
				second.outputBufferSent(channelIndex, currentAmountTransmitted);
			}

			@Override
			public void recordEmitted(int outputChannel,
					AbstractTaggableRecord record) {

				first.recordEmitted(outputChannel, record);
				second.recordEmitted(outputChannel, record);
			}
		};
	}

}
