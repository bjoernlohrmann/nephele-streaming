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

package eu.stratosphere.nephele.io.channels.bytebuffered;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.io.InputChannelResult;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.types.Record;

/**
 * @param <T> The type of record that can be transported through this channel.
 */
public abstract class AbstractByteBufferedInputChannel<T extends Record> extends AbstractInputChannel<T> {

	/**
	 * The log object used to report warnings and errors.
	 */
	private static final Log LOG = LogFactory.getLog(AbstractByteBufferedInputChannel.class);

	/**
	 * The deserializer used to deserialize records.
	 */
	private final RecordDeserializer<T> deserializer;

	/**
	 * Buffer for the uncompressed (raw) data.
	 */
	private Buffer dataBuffer;
	
	private int recordsReadFromBuffer;
	
	private long bufferInterarrivalTimeNanos;

	private ByteBufferedInputChannelBroker inputChannelBroker;
	
	private AbstractTaskEvent currentEvent;

	/**
	 * The exception observed in this channel while processing the buffers. Checked and thrown
	 * per-buffer.
	 */
	private volatile IOException ioException;

	/**
	 * Stores the number of bytes read through this input channel since its instantiation.
	 */
	private long amountOfDataTransmitted;
	

	private volatile boolean brokerAggreedToCloseChannel;

	/**
	 * Creates a new input channel.
	 * 
	 * @param inputGate
	 *        the input gate this channel is wired to
	 * @param channelIndex
	 *        the channel's index at the associated input gate
	 * @param type
	 *        the type of record transported through this channel
	 * @param channelID
	 *        the ID of the channel
	 * @param connectedChannelID
	 *        the ID of the channel this channel is connected to
	 */
	public AbstractByteBufferedInputChannel(final InputGate<T> inputGate, final int channelIndex,
			final RecordDeserializer<T> deserializer, final ChannelID channelID, final ChannelID connectedChannelID) {
		super(inputGate, channelIndex, channelID, connectedChannelID);
		this.deserializer = deserializer;
	}

	@Override
	public InputChannelResult readRecord(T target) throws IOException {
		if (this.dataBuffer == null) {
			if (isClosed()) {
				return InputChannelResult.END_OF_STREAM;
			}

			// get the next element we need to handle (buffer or event)
			BufferOrEvent boe = this.inputChannelBroker.getNextBufferOrEvent();
			
			// handle events
			if (boe.isEvent())
			{
				// sanity check: an event may only come after a complete record.
				if (this.deserializer.hasUnfinishedData()) {
					throw new IOException("Channel received an event before completing the current partial record.");
				}
				
				AbstractEvent evt = boe.getEvent();
				if (evt instanceof ByteBufferedChannelCloseEvent) {
					this.brokerAggreedToCloseChannel = true;
					return InputChannelResult.END_OF_STREAM;
				} else if (evt instanceof ChannelUnsuspendEvent) {
					this.getInputGate().setInputChannelSuspended(
							getChannelIndex(), false);
					return InputChannelResult.NONE;
				} else if (evt instanceof ChannelSuspendEvent) {
					this.getInputGate().setInputChannelSuspended(
							getChannelIndex(), true);
					try {
						// confirm channel suspend
						transferEvent(new ChannelSuspendConfirmEvent());
					} catch (InterruptedException e) {
						LOG.error("Interrupted Exception while confirming channel suspension", e);
					}
					return InputChannelResult.NONE;
				} else if (evt instanceof ChannelSuspendConfirmEvent) {
					this.getInputGate().setInputChannelSuspended(getChannelIndex(), true);
					return InputChannelResult.NONE;
				} else if (evt instanceof AbstractTaskEvent) {
					this.currentEvent = (AbstractTaskEvent) evt;
					return InputChannelResult.EVENT;
				}
				else {
					LOG.error("Received unknown event: " + evt);
					return InputChannelResult.NONE;
				}
			} else {
				// buffer case
				this.dataBuffer = boe.getBuffer();
				this.recordsReadFromBuffer = 0;
				this.bufferInterarrivalTimeNanos = boe.getInterarrivalTimeNanos();
			}
		}

		// get the next record form the buffer
		T nextRecord = this.deserializer.readData(target, this.dataBuffer);

		// release the buffer if it is empty
		if (this.dataBuffer.remaining() == 0) {
			releaseConsumedDataBuffer();
			return nextRecord == null ? InputChannelResult.NONE : InputChannelResult.LAST_RECORD_FROM_BUFFER;
		} else {
			if(nextRecord == null) {
				throw new RuntimeException("This should never happen!");
			}
			
			return nextRecord == null ? InputChannelResult.NONE : InputChannelResult.INTERMEDIATE_RECORD_FROM_BUFFER;
		}
	}

	@Override
	public boolean isClosed() throws IOException{
		if (this.ioException != null) {
			throw new IOException("An error occurred in the channel: " + this.ioException.getMessage(), this.ioException);
		} else {
			return this.brokerAggreedToCloseChannel;
		}
	} 

	@Override
	public void close() throws IOException, InterruptedException {

		this.deserializer.clear();
		if (this.dataBuffer != null) {
			releaseConsumedDataBuffer();
		}

		// This code fragment makes sure the isClosed method works in case the channel input has not been fully consumed
		while (!this.brokerAggreedToCloseChannel)
		{
			BufferOrEvent next = this.inputChannelBroker.getNextBufferOrEvent();
			if (next != null) {
				if (next.isEvent()) {
					if (next.getEvent() instanceof ByteBufferedChannelCloseEvent) {
						this.brokerAggreedToCloseChannel = true;
					}
				} else {
					next.getBuffer().recycleBuffer();
				}
			} else {
				Thread.sleep(200);
			}
		}

		// Send close event to indicate the input channel has successfully
		// processed all data it is interested in.
		transferEvent(new ByteBufferedChannelCloseEvent());
	}

	
	private void releaseConsumedDataBuffer() {
		this.amountOfDataTransmitted += this.dataBuffer.size();
		notifyDataUnitConsumed();
		this.dataBuffer = null;
		this.recordsReadFromBuffer = 0;
		this.bufferInterarrivalTimeNanos = -1;
	}

	public void setInputChannelBroker(ByteBufferedInputChannelBroker inputChannelBroker) {
		this.inputChannelBroker = inputChannelBroker;
	}


	public long notifyGateThatInputIsAvailable() {
		return this.getInputGate().notifyRecordIsAvailable(getChannelIndex());
	}

	
	@Override
	public void transferEvent(AbstractEvent event) throws IOException, InterruptedException {
		this.inputChannelBroker.transferEventToOutputChannel(event);
	}

	
	public void reportIOException(IOException ioe) {
		this.ioException = ioe;
	}

	
	@Override
	public void releaseAllResources() {
		this.brokerAggreedToCloseChannel = true;
		this.deserializer.clear();

		// The buffers are recycled by the input channel wrapper
	}	

	
	@Override
	public long getAmountOfDataTransmitted() {
		return this.amountOfDataTransmitted;
	}

	
	/**
	 * Notify the channel that a data unit has been consumed.
	 */
	public void notifyDataUnitConsumed() {
		this.getInputGate().notifyDataUnitConsumed(getChannelIndex(), bufferInterarrivalTimeNanos, recordsReadFromBuffer);
	}
	
	@Override
	public AbstractTaskEvent getCurrentEvent() {
		AbstractTaskEvent e = this.currentEvent;
		this.currentEvent = null;
		return e;
	}
}
