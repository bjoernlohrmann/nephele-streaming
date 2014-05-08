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

package eu.stratosphere.nephele.streaming.taskmanager.runtime.io;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.io.InputChannelResult;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.RecordAvailabilityListener;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.bytebuffered.InMemoryInputChannel;
import eu.stratosphere.nephele.io.channels.bytebuffered.NetworkInputChannel;
import eu.stratosphere.nephele.plugins.wrapper.AbstractInputGateWrapper;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.listener.InputGateQosReportingListener;
import eu.stratosphere.nephele.types.AbstractTaggableRecord;
import eu.stratosphere.nephele.types.Record;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wraps Nephele's {@link eu.stratosphere.nephele.io.RuntimeInputGate} to
 * intercept methods calls necessary for Qos statistics collection.
 *
 * @param <T>
 * @author Bjoern Lohrmann
 */
public final class StreamInputGate<T extends Record> extends
    AbstractInputGateWrapper<T> {

  private final static Logger LOG = Logger.getLogger(StreamInputGate.class);

  private final InputChannelChooser channelChooser;

  private HashMap<ChannelID, AbstractInputChannel<T>> inputChannels;

  private AtomicBoolean taskThreadHalted = new AtomicBoolean(false);

  private volatile InputGateQosReportingListener qosCallback;

  private AbstractTaskEvent currentEvent;

  public StreamInputGate(final InputGate<T> wrappedInputGate) {
    super(wrappedInputGate);
    this.channelChooser = new InputChannelChooser();
    this.inputChannels = new HashMap<ChannelID, AbstractInputChannel<T>>();
  }

  public void setQosReportingListener(
      InputGateQosReportingListener qosCallback) {
    this.qosCallback = qosCallback;
  }

  public InputGateQosReportingListener getQosReportingListener() {
    return this.qosCallback;
  }


  @Override
  public boolean hasInputAvailable() throws InterruptedException {
    return this.channelChooser.hasChannelAvailable();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public InputChannelResult readRecord(final T target) throws IOException,
      InterruptedException {

    if (this.isClosed()) {
      return InputChannelResult.END_OF_STREAM;
    }

    if (Thread.interrupted()) {
      throw new InterruptedException();
    }

    int channelToReadFrom = -1;

    while (channelToReadFrom == -1) {
      channelToReadFrom = this.channelChooser.chooseNextAvailableChannel();

      if (channelToReadFrom == -1) {
        // traps task thread because it is inside a chain
        this.trapTaskThreadUntilWokenUp();
      }
    }

    InputChannelResult result = this.getInputChannel(channelToReadFrom).readRecord(target);
    switch (result) {
      case INTERMEDIATE_RECORD_FROM_BUFFER:
        this.reportRecordReceived(target, channelToReadFrom);
        return InputChannelResult.INTERMEDIATE_RECORD_FROM_BUFFER;
      case LAST_RECORD_FROM_BUFFER:
        this.reportRecordReceived(target, channelToReadFrom);
        this.channelChooser.decreaseAvailableInputOnCurrentChannel();
        return InputChannelResult.LAST_RECORD_FROM_BUFFER;
      case EVENT:
        this.channelChooser.decreaseAvailableInputOnCurrentChannel();
        this.currentEvent = this.getInputChannel(channelToReadFrom).getCurrentEvent();
        return InputChannelResult.EVENT;
      case NONE:
        this.channelChooser.decreaseAvailableInputOnCurrentChannel();
        return InputChannelResult.NONE;
      case END_OF_STREAM:
        this.channelChooser.setNoAvailableInputOnCurrentChannel();
        return isClosed() ? InputChannelResult.END_OF_STREAM
            : InputChannelResult.NONE;
      default: // silence the compiler
        throw new RuntimeException();
    }
  }

  /**
   * @param record          The record that has been received.
   * @param sourceChannelID The ID of the source channel (output channel)
   */
  public void reportRecordReceived(Record record, int inputChannel) {
    if (this.qosCallback != null) {
      AbstractTaggableRecord taggableRecord = (AbstractTaggableRecord) record;
      this.qosCallback.recordReceived(inputChannel, taggableRecord);
    }
  }

  /**
   * This method should only be called if this input gate is inside a chain
   * and the task thread (doing this call) should therefore be halted (unless
   * interrupted).
   *
   * @param target
   * @throws InterruptedException if task thread is interrupted.
   */
  private void trapTaskThreadUntilWokenUp() throws InterruptedException {
    synchronized (this.taskThreadHalted) {
      this.taskThreadHalted.set(true);
      this.taskThreadHalted.notify();
      LOG.info("Task thread " + Thread.currentThread().getName() + " has halted");
      this.taskThreadHalted.wait();
      LOG.info("Task thread " + Thread.currentThread().getName() + " is awake again.");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void notifyRecordIsAvailable(final int channelIndex) {
    this.channelChooser.increaseAvailableInput(channelIndex);
    RecordAvailabilityListener<T> listener = this.getRecordAvailabilityListener();
    if (listener != null) {
      listener.reportRecordAvailability(this);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerRecordAvailabilityListener(
      final RecordAvailabilityListener<T> listener) {
    this.getWrappedInputGate().registerRecordAvailabilityListener(listener);
  }

  @Override
  public RecordAvailabilityListener<T> getRecordAvailabilityListener() {
    return this.getWrappedInputGate().getRecordAvailabilityListener();
  }

  @Override
  public void notifyDataUnitConsumed(int channelIndex) {
    this.getWrappedInputGate().notifyDataUnitConsumed(channelIndex);
  }

  public void haltTaskThreadIfNecessary() throws InterruptedException {
    this.channelChooser.setBlockIfNoChannelAvailable(false);
    synchronized (this.taskThreadHalted) {
      if (!this.taskThreadHalted.get()) {
        this.taskThreadHalted.wait();
      }
    }
  }

  public void wakeUpTaskThreadIfNecessary() {
    this.channelChooser.setBlockIfNoChannelAvailable(true);
    synchronized (this.taskThreadHalted) {
      if (this.taskThreadHalted.get()) {
        this.taskThreadHalted.notify();
      }
    }
  }

  public AbstractInputChannel<? extends Record> getInputChannel(
      ChannelID channelID) {
    return this.inputChannels.get(channelID);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public NetworkInputChannel<T> createNetworkInputChannel(
      final InputGate<T> inputGate, final ChannelID channelID,
      final ChannelID connectedChannelID) {

    NetworkInputChannel<T> channel = this.getWrappedInputGate()
        .createNetworkInputChannel(inputGate, channelID,
            connectedChannelID);

    this.inputChannels.put(channelID, channel);

    return channel;

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public InMemoryInputChannel<T> createInMemoryInputChannel(
      final InputGate<T> inputGate, final ChannelID channelID,
      final ChannelID connectedChannelID) {

    InMemoryInputChannel<T> channel = this.getWrappedInputGate()
        .createInMemoryInputChannel(inputGate, channelID,
            connectedChannelID);

    this.inputChannels.put(channelID, channel);
    return channel;
  }

  /* (non-Javadoc)
   * @see eu.stratosphere.nephele.io.InputGate#getCurrentEvent()
   */
  @Override
  public AbstractTaskEvent getCurrentEvent() {
    AbstractTaskEvent e = this.currentEvent;
    this.currentEvent = null;
    return e;
  }
}
