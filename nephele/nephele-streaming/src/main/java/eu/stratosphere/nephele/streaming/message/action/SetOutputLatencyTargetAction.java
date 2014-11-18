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

package eu.stratosphere.nephele.streaming.message.action;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.message.AbstractSerializableQosMessage;

/**
 * This class implements an action to communicate the desired output buffer
 * latency of a particular output channel to the channel itself.
 * 
 * @author warneke, Bjoern Lohrmann
 */
public final class SetOutputLatencyTargetAction extends
		AbstractSerializableQosMessage implements QosAction {

	/**
	 * The ID of the vertex the initiated action applies to.
	 */
	private final ExecutionVertexID vertexID;

	/**
	 * The ID of the output gate the channel whose buffer size shall be limited
	 * belongs to.
	 */
	private final GateID outputGateID;

	/**
	 * The ID of the output channel whose buffer size shall be limited.
	 */
	private final ChannelID sourceChannelID;

	/**
	 * The new output buffer latency target-
	 */
	private int outputBufferLatencyTarget;

	/**
	 * Constructs a new buffer size limit action object.
	 * 
	 * @param jobID
	 *            the ID of the job the action applies to
	 * @param vertexID
	 *            the ID of the vertex the action applies to
	 * @param sourceChannelID
	 *            the ID of the output channel whose buffer size shall be
	 *            limited
	 * @param outputBufferLatencyTarget
	 *            the new buffer size in bytes
	 */
	public SetOutputLatencyTargetAction(final JobID jobID,
			final ExecutionVertexID vertexID, final GateID outputGateID,
			final ChannelID sourceChannelID, final int outputBufferLatencyTarget) {
		super(jobID);

		if (vertexID == null) {
			throw new IllegalArgumentException(
					"Argument vertexID must not be null");
		}

		if (outputGateID == null) {
			throw new IllegalArgumentException(
					"Argument outputGateID must not be null");
		}

		if (sourceChannelID == null) {
			throw new IllegalArgumentException(
					"Argument sourceChannelID must not be null");
		}

		if (outputBufferLatencyTarget < 0) {
			throw new IllegalArgumentException(
					"Argument outputBufferLatencyTarget must be >= 0");
		}

		this.vertexID = vertexID;
		this.outputGateID = outputGateID;
		this.sourceChannelID = sourceChannelID;
		this.outputBufferLatencyTarget = outputBufferLatencyTarget;
	}

	/**
	 * Default constructor for deserialization.
	 */
	public SetOutputLatencyTargetAction() {
		super();
		this.vertexID = new ExecutionVertexID();
		this.outputGateID = new GateID();
		this.sourceChannelID = new ChannelID();
		this.outputBufferLatencyTarget = 0;
	}

	/**
	 * Returns the ID of the output channel whose buffer size shall be limited.
	 * 
	 * @return the ID of the output channel whose buffer size shall be limited
	 */
	public ChannelID getSourceChannelID() {

		return this.sourceChannelID;
	}

	/**
	 * Returns the new buffer size in bytes.
	 * 
	 * @return the new buffer size in bytes
	 */
	public int getOutputBufferLatencyTarget() {

		return this.outputBufferLatencyTarget;
	}

	/**
	 * Returns the ID of the vertex the initiated action applies to.
	 * 
	 * @return the ID of the vertex the initiated action applies to
	 */
	public ExecutionVertexID getVertexID() {
		return this.vertexID;
	}

	/**
	 * Returns the outputGateID.
	 * 
	 * @return the outputGateID
	 */
	public GateID getOutputGateID() {
		return this.outputGateID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		super.write(out);

		this.vertexID.write(out);
		this.outputGateID.write(out);
		this.sourceChannelID.write(out);
		out.writeInt(this.outputBufferLatencyTarget);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		super.read(in);

		this.vertexID.read(in);
		this.outputGateID.read(in);
		this.sourceChannelID.read(in);
		this.outputBufferLatencyTarget = in.readInt();
	}
}
