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
package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang3.builder.EqualsBuilder;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.channels.ChannelID;

/**
 * Identifies a Qos Reporter. The ID is deterministically constructed from the
 * Qos graph member element (vertex/edge) that it reports on.
 * 
 * @author Bjoern Lohrmann
 */
public abstract class QosReporterID implements IOReadableWritable {

	public static class Vertex extends QosReporterID {

		private ExecutionVertexID vertexID;
		private GateID inputGateID;
		private GateID outputGateID;
		private int precomputedHash;

		/**
		 * Public constructor only for deserialization.
		 */
		public Vertex() {
		}

		/**
		 * Creates a new Qos reporter ID. Initializes Vertex.
		 * 
		 * @param vertexID
		 *            The ID of the vertex that is reported on.
		 * @param inputGateID
		 *            The ID of vertex's input gate that is reported on. May be
		 *            null for dummy reporters.
		 * @param outputGateID
		 *            The ID of vertex's output gate that is reported on. May be
		 *            null for dummy reporters.
		 */
		public Vertex(ExecutionVertexID vertexID, GateID inputGateID,
				GateID outputGateID) {

			this.vertexID = vertexID;
			this.inputGateID = inputGateID;
			this.outputGateID = outputGateID;
			this.precomputeHash();
		}

		/**
		 * Returns the vertexID.
		 * 
		 * @return the vertexID
		 */
		public ExecutionVertexID getVertexID() {
			return this.vertexID;
		}

		/**
		 * Returns the inputGateID.
		 * 
		 * @return the inputGateID
		 */
		public GateID getInputGateID() {
			return this.inputGateID;
		}

		/**
		 * Returns the outputGateID.
		 * 
		 * @return the outputGateID
		 */
		public GateID getOutputGateID() {
			return this.outputGateID;
		}
		
		public boolean hasOutputGateID() {
			return this.outputGateID != null;
		}
		
		public boolean hasInputGateID() {
			return this.inputGateID != null;
		}

		private void precomputeHash() {
			this.precomputedHash = this.vertexID.hashCode();

			if (this.inputGateID != null) {
				this.precomputedHash ^= this.inputGateID.hashCode();
			}

			if (this.outputGateID != null) {
				this.precomputedHash ^= this.outputGateID.hashCode();
			}
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput
		 * )
		 */
		@Override
		public void write(DataOutput out) throws IOException {
			this.vertexID.write(out);

			byte dummyIndicatorByte = 0;
			if (this.inputGateID != null) {
				dummyIndicatorByte |= 0x01;
			}

			if (this.outputGateID != null) {
				dummyIndicatorByte |= 0x02;
			}

			out.writeByte(dummyIndicatorByte);

			if (this.inputGateID != null) {
				this.inputGateID.write(out);
			}

			if (this.outputGateID != null) {
				this.outputGateID.write(out);
			}
		}

		public boolean isDummy() {
			return this.inputGateID == null || this.outputGateID == null;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
		 */
		@Override
		public void read(DataInput in) throws IOException {
			this.vertexID = new ExecutionVertexID();
			this.vertexID.read(in);

			byte dummyIndicatorByte = in.readByte();

			if ((dummyIndicatorByte & 0x01) == 1) {
				this.inputGateID = new GateID();
				this.inputGateID.read(in);
			}

			if ((dummyIndicatorByte & 0x02) == 2) {
				this.outputGateID = new GateID();
				this.outputGateID.read(in);
			}
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			return this.precomputedHash;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (this.getClass() != obj.getClass()) {
				return false;
			}

			Vertex other = (Vertex) obj;

			return new EqualsBuilder().append(this.vertexID, other.vertexID)
					.append(this.inputGateID, other.inputGateID)
					.append(this.outputGateID, other.outputGateID).isEquals();
		}

		@Override
		public String toString() {
			return String.format("Rep:%s-%s-%s", 
					(inputGateID != null) ? inputGateID.toString() : "none",
					this.vertexID.toString(), 
					(outputGateID != null) ? outputGateID.toString() : "none");
		}
	}

	public static class Edge extends QosReporterID {

		private ChannelID sourceChannelID;

		/**
		 * Public constructor only for deserialization.
		 */
		public Edge() {
		}

		public Edge(ChannelID sourceChannelID) {
			this.sourceChannelID = sourceChannelID;
		}

		/**
		 * Returns the sourceChannelID.
		 * 
		 * @return the sourceChannelID
		 */
		public ChannelID getSourceChannelID() {
			return this.sourceChannelID;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput
		 * )
		 */
		@Override
		public void write(DataOutput out) throws IOException {
			this.sourceChannelID.write(out);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
		 */
		@Override
		public void read(DataInput in) throws IOException {
			this.sourceChannelID = new ChannelID();
			this.sourceChannelID.read(in);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReportID
		 * #hashCode()
		 */
		@Override
		public int hashCode() {
			return this.sourceChannelID.hashCode();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReportID
		 * #equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (this.getClass() != obj.getClass()) {
				return false;
			}
			Edge other = (Edge) obj;

			return this.sourceChannelID.equals(other.sourceChannelID);
		}

		@Override
		public String toString() {
			return "Rep:" + this.sourceChannelID.toString();
		}
	}

	@Override
	public abstract int hashCode();

	@Override
	public abstract boolean equals(Object other);

	public static QosReporterID.Vertex forVertex(ExecutionVertexID vertexID,
			GateID inputGateID, GateID outputGateID) {

		return new Vertex(vertexID, inputGateID, outputGateID);
	}

	public static QosReporterID.Edge forEdge(ChannelID sourceChannelID) {
		return new Edge(sourceChannelID);
	}
}
