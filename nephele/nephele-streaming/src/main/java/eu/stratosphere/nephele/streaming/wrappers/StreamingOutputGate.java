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

package eu.stratosphere.nephele.streaming.wrappers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.compression.CompressionException;
import eu.stratosphere.nephele.plugins.wrapper.AbstractOutputGateWrapper;
import eu.stratosphere.nephele.streaming.chaining.StreamChain;
import eu.stratosphere.nephele.streaming.listeners.StreamListener;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;
import eu.stratosphere.nephele.types.Record;

public final class StreamingOutputGate<T extends Record> extends AbstractOutputGateWrapper<T> {

	private final StreamListener streamListener;

//	private long lastThroughputTimestamp = -1L;
//
//	private long[] lastSentBytes = null;

	private StreamChain streamChain = null;

	private Map<ChannelID, BufferLatency> bufferLatencyMap = new HashMap<ChannelID, BufferLatency>();
	
	private OutputGateRecordTagger<T> recordTagger;

	private class BufferLatency {

		private long lastBufferLatencyTimestamp = -1L;

		private int accumulatedLatency = 0;

		private int latencyCounter = 0;

		private BufferLatency(final long initialTimestamp) {
			this.lastBufferLatencyTimestamp = initialTimestamp;
		}

		private void addLatency(long timestamp) {

			this.accumulatedLatency += (int) (timestamp - this.lastBufferLatencyTimestamp);
			this.lastBufferLatencyTimestamp = timestamp;
			++this.latencyCounter;
		}

		private int getLatency() {

			if (this.accumulatedLatency < streamListener.getContext().getAggregationInterval()) {
				return -1;
			}

			final int latency = this.accumulatedLatency / this.latencyCounter;
			this.accumulatedLatency = 0;
			this.latencyCounter = 0;

			return latency;
		}
	}

	StreamingOutputGate(final OutputGate<T> wrappedOutputGate, final StreamListener streamListener) {
		super(wrappedOutputGate);

		if (streamListener == null) {
			throw new IllegalArgumentException("Argument streamListener must not be null");
		}

		streamListener.registerOutputGate(this);

		this.streamListener = streamListener;
		
		this.recordTagger = new OutputGateRecordTagger<T>(this, streamListener);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeRecord(final T record) throws IOException, InterruptedException {

		reportRecordEmitted(record);

		if (this.streamChain == null) {
			getWrappedOutputGate().writeRecord(record);
		} else {
			this.streamChain.writeRecord(record);
		}
	}
	
	public void reportRecordEmitted(final Record record) {
		
		this.streamListener.recordEmitted(record);
		
		AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;
		this.recordTagger.tagRecordIfNecessary(taggableRecord);
	
		// FIXME: do throughput reporting on a time basis ("once every second")
		// not based on whether records are tagged (this will create WAY to many
		// throughput reports)
//		
//		if (timestamp >= 0) {
//
//			final int numberOfOutputChannels = getNumberOfOutputChannels();
//
//			if (this.lastThroughputTimestamp < 0) {
//				// Initialize array and fill it
//				this.lastSentBytes = new long[numberOfOutputChannels];
//				for (int i = 0; i < numberOfOutputChannels; ++i) {
//					this.lastSentBytes[i] = getOutputChannel(i).getAmountOfDataTransmitted();
//				}
//			} else {
//				for (int i = 0; i < numberOfOutputChannels; ++i) {
//					final AbstractOutputChannel<? extends Record> outputChannel = getOutputChannel(i);
//					final long amountOfDataTransmitted = outputChannel.getAmountOfDataTransmitted();
//					final long dataDiff = amountOfDataTransmitted - this.lastSentBytes[i];
//					this.lastSentBytes[i] = amountOfDataTransmitted;
//					final long timeDiff = timestamp - this.lastThroughputTimestamp;
//					final double throughput = (double) (1000 * 8 * dataDiff) / (double) (1024 * 1024 * timeDiff);
//					this.streamListener.reportChannelThroughput(outputChannel.getID(), throughput);
//				}
//			}
//
//			this.lastThroughputTimestamp = timestamp;
//		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void outputBufferSent(final ChannelID channelID) {

		final long timestamp = System.currentTimeMillis();
		final BufferLatency bufferLatency = this.bufferLatencyMap.get(channelID);
		if (bufferLatency == null) {
			this.bufferLatencyMap.put(channelID, new BufferLatency(timestamp));
			return;
		}

		bufferLatency.addLatency(timestamp);
		final int latency = bufferLatency.getLatency();
		if (latency >= 0) {
			this.streamListener.reportBufferLatency(channelID, latency);
		}

		getWrappedOutputGate().outputBufferSent(channelID);
	}

	public void redirectToStreamChain(final StreamChain streamChain) throws IOException, InterruptedException {

		this.streamChain = streamChain;
	}

	@Override
	public void initializeCompressors() throws CompressionException {
		getWrappedOutputGate().initializeCompressors();
	}
}
