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

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.channels.bytebuffered.InMemoryOutputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.NetworkOutputChannel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.Record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * In Nephele output gates are a specialization of general gates and connect
 * record writers and output channels. As channels, output gates are always
 * parameterized to a specific type of record which they can transport.
 * <p>
 * This class is in general not thread-safe.
 * 
 * @author warneke
 * @param <T>
 *        the type of record that can be transported through this gate
 */
public class RuntimeOutputGate<T extends Record> extends AbstractGate<T> implements OutputGate<T> {

	/**
	 * The list of output channels attached to this gate.
	 */
	private final ArrayList<AbstractOutputChannel<T>> outputChannels = new ArrayList<AbstractOutputChannel<T>>();
	
	private volatile int activeOutputChannels = 0;

	/**
	 * Channel selector to determine which channel is supposed receive the next record.
	 */
	private final ChannelSelector<T> channelSelector;

	/**
	 * The class of the record transported through this output gate.
	 */
	private final Class<T> type;

	/**
	 * Stores whether all records passed to this output gate shall be transmitted through all connected output channels.
	 */
	private final boolean isBroadcast;
	
	/**
	 * Queue with indices of channels that have pending events.
	 */
	private final BlockingQueue<Integer> channelsWithPendingEvents = new LinkedBlockingQueue<Integer>();

	/**
	 * Constructs a new runtime output gate.
	 * 
	 * @param jobID
	 *        the ID of the job this input gate belongs to
	 * @param gateID
	 *        the ID of the gate
	 * @param inputClass
	 *        the class of the record that can be transported through this
	 *        gate
	 * @param index
	 *        the index assigned to this output gate at the {@link Environment} object
	 * @param channelSelector
	 *        the channel selector to be used for this output gate
	 * @param isBroadcast
	 *        <code>true</code> if every records passed to this output gate shall be transmitted through all connected
	 *        output channels, <code>false</code> otherwise
	 */
	public RuntimeOutputGate(final JobID jobID, final GateID gateID, final Class<T> inputClass, final int index,
			final ChannelSelector<T> channelSelector, final boolean isBroadcast) {

		super(jobID, gateID, index);

		this.isBroadcast = isBroadcast;
		this.type = inputClass;

		if (this.isBroadcast) {
			this.channelSelector = null;
		} else {
			if (channelSelector == null) {
				this.channelSelector = new DefaultChannelSelector<T>();
			} else {
				this.channelSelector = channelSelector;
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final Class<T> getType() {
		return this.type;
	}

	/**
	 * Adds a new output channel to the output gate.
	 * 
	 * @param outputChannel
	 *        the output channel to be added.
	 */
	private void addOutputChannel(AbstractOutputChannel<T> outputChannel) {
		if (!this.outputChannels.contains(outputChannel)) {
			this.outputChannels.add(outputChannel);
			this.activeOutputChannels++;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isInputGate() {

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfOutputChannels() {

		return this.outputChannels.size();
	}

	/**
	 * Returns the output channel from position <code>pos</code> of the gate's
	 * internal channel list.
	 * 
	 * @param pos
	 *        the position to retrieve the channel from
	 * @return the channel from the given position or <code>null</code> if such
	 *         position does not exist.
	 */
	public AbstractOutputChannel<T> getOutputChannel(int pos) {

		if (pos < this.outputChannels.size())
			return this.outputChannels.get(pos);
		else
			return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NetworkOutputChannel<T> createNetworkOutputChannel(final OutputGate<T> outputGate,
			final ChannelID channelID, final ChannelID connectedChannelID) {

		final NetworkOutputChannel<T> enoc = new NetworkOutputChannel<T>(outputGate, this.outputChannels.size(),
			channelID, connectedChannelID);
		addOutputChannel(enoc);

		return enoc;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InMemoryOutputChannel<T> createInMemoryOutputChannel(final OutputGate<T> outputGate,
			final ChannelID channelID, final ChannelID connectedChannelID) {

		final InMemoryOutputChannel<T> einoc = new InMemoryOutputChannel<T>(outputGate, this.outputChannels.size(),
			channelID, connectedChannelID);
		addOutputChannel(einoc);

		return einoc;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void requestClose() throws IOException, InterruptedException {
		// Close all output channels
		for (int i = 0; i < this.getNumberOfOutputChannels(); i++) {
			final AbstractOutputChannel<T> outputChannel = this.getOutputChannel(i);
			outputChannel.requestClose();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isClosed() throws IOException, InterruptedException {

		boolean allClosed = true;

		for (int i = 0; i < this.getNumberOfOutputChannels(); i++) {
			final AbstractOutputChannel<T> outputChannel = this.getOutputChannel(i);
			if (!outputChannel.isClosed()) {
				allClosed = false;
			}
		}

		return allClosed;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeRecord(final T record) throws IOException, InterruptedException {
		
		processPendingChannelEvents();

		if (this.isBroadcast) {

			if (getChannelType() == ChannelType.INMEMORY) {

				final int numberOfOutputChannels = getNumberOfActiveOutputChannels();
				for (int i = 0; i < numberOfOutputChannels; ++i) {
					this.outputChannels.get(i).writeRecord(record);
				}

			} else {

				// Use optimization for byte buffered channels
				this.outputChannels.get(0).writeRecord(record);
			}

		} else {

			// Non-broadcast gate, use channel selector to select output channels
			final int numberOfOutputChannels = getNumberOfActiveOutputChannels();
			final int[] selectedOutputChannels = this.channelSelector.selectChannels(record, numberOfOutputChannels);

			if (selectedOutputChannels == null) {
				return;
			}

			for (int i = 0; i < selectedOutputChannels.length; ++i) {

				if (selectedOutputChannels[i] < numberOfOutputChannels) {
					final AbstractOutputChannel<T> outputChannel = this.outputChannels.get(selectedOutputChannels[i]);
					outputChannel.writeRecord(record);
				}
			}
		}
	}

	private void processPendingChannelEvents() throws IOException, InterruptedException {
		while(!this.channelsWithPendingEvents.isEmpty()) {
			int channelIndex = this.channelsWithPendingEvents.poll().intValue();
			this.outputChannels.get(channelIndex).processPendingEvents();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<AbstractOutputChannel<T>> getOutputChannels() {
		return this.outputChannels;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "Output " + super.toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void publishEvent(AbstractTaskEvent event) throws IOException, InterruptedException {

		// Copy event to all connected channels
		final Iterator<AbstractOutputChannel<T>> it = this.outputChannels.iterator();
		while (it.hasNext()) {
			it.next().transferEvent(event);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void flush() throws IOException, InterruptedException {
		// Flush all connected channels
		final Iterator<AbstractOutputChannel<T>> it = this.outputChannels.iterator();
		while (it.hasNext()) {
			it.next().flush();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isBroadcast() {

		return this.isBroadcast;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ChannelSelector<T> getChannelSelector() {

		return this.channelSelector;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void releaseAllChannelResources() {

		final Iterator<AbstractOutputChannel<T>> it = this.outputChannels.iterator();

		while (it.hasNext()) {
			it.next().releaseAllResources();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void outputBufferSent(final int channelIndex) {
		// Nothing to do here
	}

	/**
	 * Marks the output channel with the given index as suspended and recomputes the gate's
	 * number of active output channels.
	 */
	@Override
	public synchronized void setOutputChannelSuspended(int index, boolean isSuspended) {

		if (isSuspended != this.getOutputChannel(index).isSuspended()) {
			if(!isSuspended) {
				// initialize autoflush interval
				int randomFlushDeadline = this.getOutputChannel(
						(int) (activeOutputChannels * Math.random()))
						.getFlushDeadline();
				this.getOutputChannel(index).setFlushDeadline(randomFlushDeadline);
			}
			this.getOutputChannel(index).setSuspended(isSuspended);
			
			switch(this.getGateState()) {
			case RUNNING:
				// channel (un)suspension may occur out of order. For example unsuspension
				// for channel 10 can occur before channel 9 is unsuspended. Unfortunately
				// the channel selector interface does not offer a way to signal which channel
				// index specifically is suspended and which not. This means that in the above example
				// channel 10 is unusable despite being unsuspended (and channel 9 unusable & suspended).
				this.activeOutputChannels = countUsableOutputChannels();
				break;
			case DRAINING:
				this.activeOutputChannels = countUnsuspendedOutputChannels();
				if (this.activeOutputChannels == 0) {
					this.updateGateState(GateState.DRAINING,
							GateState.SUSPENDED);
				}
				break;
			default:
				break;
			}
		}
	}

	private int countUnsuspendedOutputChannels() {
		int unsuspendedOutputChannels = 0;
		for (int i = 0; i < this.outputChannels.size(); i++) {
			if (!this.getOutputChannel(i).isSuspended()) {
				unsuspendedOutputChannels++;
			}
		}
		return unsuspendedOutputChannels;
	}

	private int countUsableOutputChannels() {
		int usableOutputChannels = 0;
		for (int i = 0; i < this.outputChannels.size(); i++) {
			if (this.getOutputChannel(i).isSuspended()) {
				break;
			}
			usableOutputChannels++;
		}
		return usableOutputChannels;
	}

	@Override
	public int getNumberOfActiveOutputChannels() {
		return this.activeOutputChannels;
	}

	@Override
	public void requestSuspend() throws IOException, InterruptedException {
		if (this.getGateState() != GateState.RUNNING) {
			return;
		}
		
		this.updateGateState(GateState.RUNNING, GateState.DRAINING);

		for (AbstractOutputChannel<?> outputChannel : this.outputChannels) {
			if (!outputChannel.isSuspended()) {
				outputChannel.requestSuspend();
			}
		}
	}

	@Override
	public void notifyPendingEvent(int channelIndex) {
		this.channelsWithPendingEvents.add(channelIndex);
	}

	@Override
	public void outputBufferAllocated(int channelIndex) {
		// Nothing to do here
	}
}
