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

package eu.stratosphere.nephele.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.ChannelSuspendEvent;
import eu.stratosphere.nephele.io.channels.bytebuffered.InMemoryInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.NetworkInputChannel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.Record;

/**
 * In Nephele input gates are a specialization of general gates and connect
 * input channels and record readers. As channels, input gates are always
 * parameterized to a specific type of record which they can transport. In
 * contrast to output gates input gates can be associated with a
 * {@link DistributionPattern} object which dictates the concrete wiring between
 * two groups of vertices.
 * 
 * @param <T>
 *            The type of record that can be transported through this gate.
 */
public class RuntimeInputGate<T extends Record> extends AbstractGate<T>
		implements InputGate<T> {
	
	/**
	 * The deserializer factory used to instantiate the deserializers that
	 * construct records from byte streams.
	 */
	private final RecordDeserializerFactory<T> deserializerFactory;

	/**
	 * The list of input channels attached to this input gate.
	 */
	private final ArrayList<AbstractInputChannel<T>> inputChannels = new ArrayList<AbstractInputChannel<T>>();

	private int activeInputChannels = 0;

	/**
	 * Queue with indices of channels that store at least one available record.
	 */
	private final BlockingQueue<Integer> availableChannels = new LinkedBlockingQueue<Integer>();

	/**
	 * The listener object to be notified when a channel has at least one record
	 * available.
	 */
	private final AtomicReference<RecordAvailabilityListener<T>> recordAvailabilityListener = new AtomicReference<RecordAvailabilityListener<T>>(
			null);

	private AbstractTaskEvent currentEvent;

	/**
	 * If the value of this variable is set to <code>true</code>, the input gate
	 * is closed.
	 */
	private boolean isClosed = false;

	/**
	 * The channel to read from next.
	 */
	private int channelToReadFrom = -1;

	/**
	 * Constructs a new runtime input gate.
	 * 
	 * @param jobID
	 *            the ID of the job this input gate belongs to
	 * @param gateID
	 *            the ID of the gate
	 * @param deserializerFactory
	 *            The factory used to instantiate the deserializers that
	 *            construct records from byte streams.
	 * @param index
	 *            the index assigned to this input gate at the
	 *            {@link Environment} object
	 */
	public RuntimeInputGate(final JobID jobID, final GateID gateID,
			final RecordDeserializerFactory<T> deserializerFactory,
			final int index) {
		super(jobID, gateID, index);
		this.deserializerFactory = deserializerFactory;
	}

	/**
	 * Adds a new input channel to the input gate.
	 * 
	 * @param inputChannel
	 *            the input channel to be added.
	 */
	private void addInputChannel(AbstractInputChannel<T> inputChannel) {
		// in high DOPs, this can be a serious performance issue, as adding all
		// channels and checking linearly has a
		// quadratic complexity!
		if (!this.inputChannels.contains(inputChannel)) {
			this.inputChannels.add(inputChannel);
			this.activeInputChannels++;
		}
	}

	@Override
	public boolean isInputGate() {
		return true;
	}

	@Override
	public int getNumberOfInputChannels() {
		return this.inputChannels.size();
	}

	@Override
	public AbstractInputChannel<T> getInputChannel(int pos) {
		return this.inputChannels.get(pos);
	}

	@Override
	public NetworkInputChannel<T> createNetworkInputChannel(
			final InputGate<T> inputGate, final ChannelID channelID,
			final ChannelID connectedChannelID) {

		final NetworkInputChannel<T> enic = new NetworkInputChannel<T>(
				inputGate, this.inputChannels.size(),
				this.deserializerFactory.createDeserializer(), channelID,
				connectedChannelID);
		addInputChannel(enic);

		return enic;
	}

	@Override
	public InMemoryInputChannel<T> createInMemoryInputChannel(
			final InputGate<T> inputGate, final ChannelID channelID,
			final ChannelID connectedChannelID) {

		final InMemoryInputChannel<T> eimic = new InMemoryInputChannel<T>(
				inputGate, this.inputChannels.size(),
				this.deserializerFactory.createDeserializer(), channelID,
				connectedChannelID);
		addInputChannel(eimic);

		return eimic;
	}

	@Override
	public boolean hasInputAvailable() {
		return this.channelToReadFrom != -1
				|| !this.availableChannels.isEmpty();
	}

	@Override
	public InputChannelResult readRecord(T target) throws IOException,
			InterruptedException {

		this.handleGateState();
		
		if (this.channelToReadFrom == -1) {
			if (this.isClosed()) {
				return InputChannelResult.END_OF_STREAM;
			}

			if (Thread.interrupted()) {
				throw new InterruptedException();
			}

			this.channelToReadFrom = waitForAnyChannelToBecomeAvailable();
		}

		InputChannelResult result = this
				.getInputChannel(this.channelToReadFrom).readRecord(target);
		switch (result) {
		case INTERMEDIATE_RECORD_FROM_BUFFER: // full record and we can stay on
												// the same channel
			return InputChannelResult.INTERMEDIATE_RECORD_FROM_BUFFER;

		case LAST_RECORD_FROM_BUFFER: // full record, but we must switch the
										// channel afterwards
			this.channelToReadFrom = -1;
			return InputChannelResult.LAST_RECORD_FROM_BUFFER;

		case EVENT: // task event
			this.currentEvent = this.getInputChannel(this.channelToReadFrom)
					.getCurrentEvent();
			this.channelToReadFrom = -1; // event always marks a unit as
											// consumed
			return InputChannelResult.EVENT;

		case NONE: 
			// internal event or an incomplete record that needs further
			// chunks the current unit is exhausted
			this.channelToReadFrom = -1;
			this.handleGateState();
			
			// handleGateState() can declare the gate closed
			if (this.isClosed()) {
				return InputChannelResult.END_OF_STREAM;
			} else {
				return InputChannelResult.NONE;
			}

		case END_OF_STREAM: // channel is done
			this.channelToReadFrom = -1;
			return isClosed() ? InputChannelResult.END_OF_STREAM
					: InputChannelResult.NONE;

		default: // silence the compiler
			throw new RuntimeException();
		}
	}

	@Override
	public AbstractTaskEvent getCurrentEvent() {
		AbstractTaskEvent e = this.currentEvent;
		this.currentEvent = null;
		return e;
	}

	@Override
	public long notifyRecordIsAvailable(int channelIndex) {
		this.availableChannels.add(Integer.valueOf(channelIndex));

		RecordAvailabilityListener<T> listener = this.recordAvailabilityListener
				.get();
		if (listener != null) {
			listener.reportRecordAvailability(this);
		}
		
		return -1;
	}

	/**
	 * This method returns the index of a channel which has at least one record
	 * available. The method may block until at least one channel has become
	 * ready.
	 * 
	 * @return the index of the channel which has at least one record available
	 */
	public int waitForAnyChannelToBecomeAvailable() throws InterruptedException {
		return this.availableChannels.take().intValue();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isClosed() throws IOException, InterruptedException {

		if (this.isClosed) {
			return true;
		}

		for (int i = 0; i < this.getNumberOfInputChannels(); i++) {
			final AbstractInputChannel<T> inputChannel = this.inputChannels
					.get(i);
			if (!inputChannel.isClosed()) {
				return false;
			}
		}

		this.isClosed = true;

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException, InterruptedException {

		for (int i = 0; i < this.getNumberOfInputChannels(); i++) {
			final AbstractInputChannel<T> inputChannel = this.inputChannels
					.get(i);
			inputChannel.close();
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "Input " + super.toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void publishEvent(AbstractTaskEvent event) throws IOException,
			InterruptedException {

		// Copy event to all connected channels
		for (AbstractInputChannel<?> inputChannel : this.inputChannels) {
			if (!inputChannel.isSuspended()) {
				inputChannel.transferEvent(event);
			}
		}
	}

	/**
	 * Returns the {@link RecordDeserializerFactory} used by this input gate.
	 * 
	 * @return The {@link RecordDeserializerFactory} used by this input gate.
	 */
	public RecordDeserializerFactory<T> getRecordDeserializerFactory() {
		return this.deserializerFactory;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void releaseAllChannelResources() {

		final Iterator<AbstractInputChannel<T>> it = this.inputChannels
				.iterator();
		while (it.hasNext()) {
			it.next().releaseAllResources();
		}
	}

	@Override
	public void registerRecordAvailabilityListener(
			final RecordAvailabilityListener<T> listener) {
		if (!this.recordAvailabilityListener.compareAndSet(null, listener)) {
			throw new IllegalStateException(
					this.recordAvailabilityListener
							+ " is already registered as a record availability listener");
		}
	}

	@Override
	public RecordAvailabilityListener<T> getRecordAvailabilityListener() {
		return this.recordAvailabilityListener.get();
	}

	public void notifyDataUnitConsumed(int channelIndex, long interarrivalTimeNanos, int recordsReadFromBuffer) {
		// do nothing
	}

	@Override
	public void setInputChannelSuspended(int index, boolean isSuspended) {
		if (isSuspended != this.getInputChannel(index).isSuspended()) {
			this.getInputChannel(index).setSuspended(isSuspended);
			
			this.activeInputChannels = (isSuspended) ? this.activeInputChannels - 1
					: this.activeInputChannels + 1;
		}
	}

	@Override
	public int getNumberOfActiveInputChannels() {
		return this.activeInputChannels;
	}

	@Override
	public void requestSuspend() throws IOException, InterruptedException {
		this.updateGateState(GateState.RUNNING, GateState.SUSPENDING);
	}

	@Override
	public void handleGateState() throws InterruptedException, IOException {
		switch (this.getGateState()) {
		case RUNNING:
			if (this.activeInputChannels == 0) {
				this.updateGateState(GateState.RUNNING, GateState.SUSPENDED);
				this.isClosed = true;
			}
			break;
		case SUSPENDING:
			for (AbstractInputChannel<?> inputChannel : this.inputChannels) {
				if (!inputChannel.isSuspended()) {
					inputChannel.transferEvent(new ChannelSuspendEvent());
				}
			}
			this.updateGateState(GateState.SUSPENDING, GateState.DRAINING);
			break;
		case DRAINING:
			if (this.activeInputChannels == 0) {
				this.updateGateState(GateState.DRAINING, GateState.SUSPENDED);
				this.isClosed = true;
			}
			break;
		default:
			break;
		}
	}
}
