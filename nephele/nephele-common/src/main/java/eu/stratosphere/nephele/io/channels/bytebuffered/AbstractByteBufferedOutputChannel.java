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

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.*;
import eu.stratosphere.nephele.types.Record;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class AbstractByteBufferedOutputChannel<T extends Record> extends AbstractOutputChannel<T> {
	
	private static final ScheduledThreadPoolExecutor scheduledFlusherThreadPool = new ScheduledThreadPoolExecutor(20);
	
	private class DataBufferFlusher implements Runnable {
		private final Buffer bufferToFlush;
		
		public DataBufferFlusher(Buffer bufferToFlush) {
			this.bufferToFlush = bufferToFlush;
		}

		@Override
		public void run() {
			synchronized (AbstractByteBufferedOutputChannel.this) {
				if (dataBuffer == bufferToFlush) {
					try {
						flushBufferUnsynchronized();
					} catch (IOException e) {
						LOG.error("Error in flusher thread: " + e.getMessage(),
								e);
					} catch (InterruptedException e) {
						// do nothing
					}
				}
			}
		}
	}

	/**
	 * The serialization buffer used to serialize records.
	 */
	private final SerializationBuffer<T> serializationBuffer = new SerializationBuffer<T>();
	
	/**
	 * Buffer for the serialized output data.
	 */
	private Buffer dataBuffer = null;

	private int flushDeadline = 0;

	/**
	 * Stores whether the channel is requested to be closed.
	 */
	private boolean closeRequested = false;
	
	private volatile boolean suspendRequested = false;
	
	private volatile boolean receivedChannelSuspendEvent = false;

	/**
	 * The output channel broker the channel should contact to request and release write buffers.
	 */
	private ByteBufferedOutputChannelBroker outputChannelBroker = null;


	/**
	 * Stores the number of bytes transmitted through this output channel since its instantiation.
	 */
	private long amountOfDataTransmitted = 0L;

	private static final Log LOG = LogFactory.getLog(AbstractByteBufferedOutputChannel.class);

	/**
	 * Creates a new byte buffered output channel.
	 * 
	 * @param outputGate
	 *        the output gate this channel is wired to
	 * @param channelIndex
	 *        the channel's index at the associated output gate
	 * @param channelID
	 *        the ID of the channel
	 * @param connectedChannelID
	 *        the ID of the channel this channel is connected to
	 */
	protected AbstractByteBufferedOutputChannel(final OutputGate<T> outputGate, final int channelIndex,
			final ChannelID channelID, final ChannelID connectedChannelID) {
		super(outputGate, channelIndex, channelID, connectedChannelID);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized boolean isClosed() throws IOException, InterruptedException {
		return this.closeRequested && this.dataBuffer == null
				&& !this.serializationBuffer.dataLeftFromPreviousSerialization()
				&& !this.outputChannelBroker.hasDataLeftToTransmit();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void requestClose() throws IOException, InterruptedException {
		if (!this.closeRequested) {
			this.closeRequested = true;
			flush();

			if (getType() == ChannelType.INMEMORY || !isBroadcastChannel() || getChannelIndex() == 0) {
				transferEvent(new ByteBufferedChannelCloseEvent());
			}
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void requestSuspend() throws IOException, InterruptedException {

		if (!this.suspendRequested) {
			this.suspendRequested = true;
			transferEvent(new ChannelSuspendEvent());
		}
	}


	/**
	 * Requests a new write buffer from the framework. This method blocks until the requested buffer is available.
	 * 
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the buffer
	 * @throws IOException
	 *         thrown if an I/O error occurs while waiting for the buffer
	 */
	private Buffer requestWriteBufferFromBroker() throws InterruptedException, IOException {
		if (Thread.interrupted()) {
			throw new InterruptedException();
		}

		Buffer ret = this.outputChannelBroker.requestEmptyWriteBuffer();
		// Notify the output gate to enable statistics collection by plugins
		getOutputGate().outputBufferAllocated(getChannelIndex());

		return ret;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeRecord(T record) throws IOException, InterruptedException {

		if (this.closeRequested || this.suspendRequested) {
			throw new IOException("Channel is already requested to be closed/suspended");
		}

		if (this.serializationBuffer.dataLeftFromPreviousSerialization()) {
			throw new IOException(
					"Serialization buffer is expected to be empty!");
		}

		this.serializationBuffer.serialize(record);
		
		flushSerializationBuffer(flushDeadline == 0);
	}
	
	/**
	 * Returns the filled buffer to the framework and triggers further processing. This method by itself
	 * is not thread-safe.
	 * 
	 * @throws IOException
	 *         thrown if an I/O error occurs while releasing the buffers
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while releasing the buffers
	 */
	private void flushBufferUnsynchronized() throws IOException, InterruptedException {
		if (this.dataBuffer != null) {
			this.outputChannelBroker.releaseWriteBuffer(dataBuffer);

			// Notify the output gate to enable statistics collection by plugins
			getOutputGate().outputBufferSent(getChannelIndex());

			this.dataBuffer = null;
		}
	}
	
	private synchronized void flushSerializationBuffer(boolean releaseNonEmptyDataBuffer) throws InterruptedException, IOException {
		boolean freshBufferAllocated = false;
		
		while (this.serializationBuffer.dataLeftFromPreviousSerialization()) {
			if (this.dataBuffer == null) {
				this.dataBuffer = requestWriteBufferFromBroker();
				freshBufferAllocated = true;
			}
			
			this.amountOfDataTransmitted += this.serializationBuffer.read(this.dataBuffer);
			if (this.dataBuffer.remaining() == 0) {
				flushBufferUnsynchronized();				
			}
		}
		
		if (this.dataBuffer != null) {
			if (releaseNonEmptyDataBuffer) {
				flushBufferUnsynchronized();
			} else if (freshBufferAllocated) {
				scheduledFlusherThreadPool.schedule(new DataBufferFlusher(this.dataBuffer),
								flushDeadline,
								TimeUnit.MILLISECONDS);
			}
		}
	}
	
	
	/**
	 * Sets the output channel broker this channel should contact to request and release write buffers.
	 * 
	 * @param byteBufferedOutputChannelBroker
	 *        the output channel broker the channel should contact to request and release write buffers
	 */
	public void setByteBufferedOutputChannelBroker(ByteBufferedOutputChannelBroker byteBufferedOutputChannelBroker) {

		this.outputChannelBroker = byteBufferedOutputChannelBroker;
	}


	public void processEvent(AbstractEvent event) {

		if (event instanceof AbstractTaskEvent) {
			getOutputGate().deliverEvent((AbstractTaskEvent) event);
		} else if (event instanceof ChannelSuspendConfirmEvent) {
			if (this.suspendRequested) {
				getOutputGate().setOutputChannelSuspended(
						this.getChannelIndex(), true);
			} else {
				LOG.error("Received unsolicited ChannelSuspendConfirmEvent");
			}
		} else if (event instanceof ChannelSuspendEvent) {
			// ChannelSuspendEvent needs to be confirmed by task thread
			// otherwise we risk a race condition where task thread writes
			// to the channel after confirmation has been sent.
			this.receivedChannelSuspendEvent = true;
			getOutputGate().notifyPendingEvent(getChannelIndex());
		} else if (event instanceof ChannelUnsuspendEvent) {
			getOutputGate().setOutputChannelSuspended(this.getChannelIndex(),
					false);
		} else {
			LOG.error("Channel " + getID() + " received unknown event " + event);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void transferEvent(AbstractEvent event) throws IOException, InterruptedException {
		flush();
		this.outputChannelBroker.transferEventToInputChannel(event);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void flush() throws IOException, InterruptedException {
		flushSerializationBuffer(true);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void releaseAllResources() {

		// TODO: Reconsider release of broker's resources here
		this.closeRequested = true;
		
		this.serializationBuffer.clear();
		if (dataBuffer != null) {
			dataBuffer.recycleBuffer();
			dataBuffer = null;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getAmountOfDataTransmitted() {

		return this.amountOfDataTransmitted;
	}
	
	public void processPendingEvents() throws IOException, InterruptedException {
		// channel suspends need to be confirmed by the task thread
		// but we cannot do that in channel.write() because the channel
		// may never be written to (hence no confirmation is sent ever)
		if (this.receivedChannelSuspendEvent) {
			getOutputGate().setOutputChannelSuspended(this.getChannelIndex(), true);
			// send confirmation
			transferEvent(new ChannelSuspendConfirmEvent());
			this.receivedChannelSuspendEvent = false;
		}
	}
	
	public void setFlushDeadline(int flushDeadline) {
		this.flushDeadline = flushDeadline;
	}
	
	public int getFlushDeadline() {
		return this.flushDeadline;
	}

	public synchronized static void ensureAutoflushThreadPoolsize(int poolsize) {
		if (scheduledFlusherThreadPool.getCorePoolSize() != poolsize) {
			scheduledFlusherThreadPool.setCorePoolSize(poolsize);
		}
	}
}
