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

package eu.stratosphere.nephele.streaming.taskmanager.runtime;

import eu.stratosphere.nephele.execution.RuntimeEnvironment;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.DefaultChannelSelector;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.RecordDeserializerFactory;
import eu.stratosphere.nephele.plugins.wrapper.EnvironmentWrapper;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamChannelSelector;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamInputGate;
import eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamOutputGate;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.ioc.IocTask;
import eu.stratosphere.nephele.types.Record;

/**
 * A StreamTaskEnvironment has task-scope and wraps the created input and output
 * gates in special {@link StreamInputGate} and {@link StreamOutputGate}
 * objects to intercept methods calls necessary for Qos statistics collection.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke, Bjoern Lohrmann
 */
public final class StreamTaskEnvironment extends EnvironmentWrapper {

	/**
	 * The ID of the respective execution vertex. Unfortunately the wrapped
	 * environment does not have this information.
	 */
	private ExecutionVertexID vertexID;

	/**
	 * Constructs a new streaming environment
	 * 
	 * @param wrappedEnvironment
	 *            the environment to be encapsulated by this streaming
	 *            environment
	 */
	public StreamTaskEnvironment(final RuntimeEnvironment wrappedEnvironment) {
		super(wrappedEnvironment);

	}

	/**
	 * @return the ID of the respective execution vertex this environment
	 *         belongs to.
	 */
	public ExecutionVertexID getVertexID() {
		return this.vertexID;
	}

	/**
	 * Sets the ID of the respective execution vertex this environment belongs
	 * to.
	 * 
	 * @param vertexID
	 *            the ID to set
	 */
	public void setVertexID(ExecutionVertexID vertexID) {
		if (vertexID == null) {
			throw new NullPointerException("vertexID must not be null");
		}

		this.vertexID = vertexID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <T extends Record> OutputGate<T> createOutputGate(
			final GateID gateID, final Class<T> outputClass,
			ChannelSelector<T> selector, final boolean isBroadcast) {

		StreamChannelSelector<T> wrappedSelector;
		if (selector == null) {
			wrappedSelector = new StreamChannelSelector<T>(
					new DefaultChannelSelector<T>());
		} else {
			wrappedSelector = new StreamChannelSelector<T>(selector);
		}

		OutputGate<T> outputGate = this.getWrappedEnvironment()
				.createOutputGate(gateID, outputClass, wrappedSelector,
						isBroadcast);
		return new StreamOutputGate<T>(outputGate, wrappedSelector);
	}

	public boolean isIocTask() {
		return getIocTask() != null;
	}

	public IocTask getIocTask() {
		AbstractInvokable invokable = ((RuntimeEnvironment) getWrappedEnvironment()).getInvokable();
		if (!(invokable instanceof StreamTaskWrapper)) {
			return null;
		}
		invokable = ((StreamTaskWrapper) invokable).getWrappedInvokable();
		if (invokable instanceof IocTask) {
			return (IocTask) invokable;
		}
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <T extends Record> InputGate<T> createInputGate(final GateID gateID,
			final RecordDeserializerFactory<T> deserializer) {

		InputGate<T> inputGate = this.getWrappedEnvironment().createInputGate(
				gateID, deserializer);

		return new StreamInputGate<T>(inputGate);
	}

	public StreamInputGate<? extends Record> getInputGate(GateID gateID) {
		for (int i = 0; i < this.getNumberOfInputGates(); i++) {
			StreamInputGate<? extends Record> inputGate = this.getInputGate(i);
			if (inputGate.getGateID().equals(gateID)) {
				return inputGate;
			}
		}

		return null;
	}

	public StreamOutputGate<? extends Record> getOutputGate(GateID gateID) {
		for (int i = 0; i < this.getNumberOfOutputGates(); i++) {
			StreamOutputGate<? extends Record> outputGate = this
					.getOutputGate(i);
			if (outputGate.getGateID().equals(gateID)) {
				return outputGate;
			}
		}

		return null;
	}

	public StreamInputGate<? extends Record> getInputGate(int gateIndex) {
		return (StreamInputGate<? extends Record>) ((RuntimeEnvironment) this
				.getWrappedEnvironment()).getInputGate(gateIndex);
	}

	public StreamOutputGate<? extends Record> getOutputGate(int gateIndex) {
		return (StreamOutputGate<? extends Record>) ((RuntimeEnvironment) this
				.getWrappedEnvironment()).getOutputGate(gateIndex);
	}
}
