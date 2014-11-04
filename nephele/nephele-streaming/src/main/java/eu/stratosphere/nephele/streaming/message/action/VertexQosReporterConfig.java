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

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.SamplingStrategy;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGate;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Describes a Qos reporter role for a vertex (task).
 *
 * @author Bjoern Lohrmann
 */
public class VertexQosReporterConfig implements IOReadableWritable {

	private JobVertexID groupVertexID;

	private ExecutionVertexID vertexID;

	private InstanceConnectionInfo reporterInstance;

	private InstanceConnectionInfo[] qosManagers;

	private int inputGateIndex;

	private GateID inputGateID;

	private int outputGateIndex;

	private GateID outputGateID;

	private SamplingStrategy samplingStrategy;

	private int memberIndex;

	private String name;

	public VertexQosReporterConfig() {
	}

	/**
	 * Initializes VertexQosReporterConfig. Dummy reporter configs can be
	 * created by prividing -1 gate indices and null gate IDs.
	 *
	 * @param groupVertexID
	 * @param vertexID
	 * @param qosManagers
	 * @param inputGateIndex
	 *            If inputGateIndex is -1, inputGateID must be null (and vice
	 *            versa).
	 * @param inputGateID
	 * @param outputGateIndex
	 *            If outputGateIndex is -1, outputGateID must be null (and vice
	 *            versa).
	 * @param outputGateID
	 * @param samplingStrategy
	 * @param memberIndex
	 * @param name
	 */
	public VertexQosReporterConfig(JobVertexID groupVertexID,
			ExecutionVertexID vertexID,
			InstanceConnectionInfo reporterInstance,
			InstanceConnectionInfo[] qosManagers, int inputGateIndex,
			GateID inputGateID, int outputGateIndex, GateID outputGateID,
			SamplingStrategy samplingStrategy, int memberIndex, String name) {

		if (inputGateIndex == -1 ^ inputGateID == null || outputGateIndex == -1 ^ outputGateID == null) {

			throw new RuntimeException(
					"If inputGateIndex/outputGateIndex is (not) -1, the respective gate ID must (not) be null. This is a bug.");
		}

		this.groupVertexID = groupVertexID;
		this.vertexID = vertexID;
		this.reporterInstance = reporterInstance;
		this.qosManagers = qosManagers;
		this.inputGateIndex = inputGateIndex;
		this.inputGateID = inputGateID;
		this.outputGateIndex = outputGateIndex;
		this.outputGateID = outputGateID;
		this.samplingStrategy = samplingStrategy;
		this.memberIndex = memberIndex;
		this.name = name;
	}

	public boolean isDummy() {
		return this.getReporterID().isDummy();
	}

	/**
	 * Returns the groupVertexID.
	 *
	 * @return the groupVertexID
	 */
	public JobVertexID getGroupVertexID() {
		return this.groupVertexID;
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
	 * Returns the reporterInstance.
	 *
	 * @return the reporterInstance
	 */
	public InstanceConnectionInfo getReporterInstance() {
		return this.reporterInstance;
	}

	/**
	 * Returns the qosManagers.
	 *
	 * @return the qosManagers
	 */
	public InstanceConnectionInfo[] getQosManagers() {
		return this.qosManagers;
	}

	/**
	 * Returns the inputGateIndex.
	 *
	 * @return the inputGateIndex
	 */
	public int getInputGateIndex() {
		return this.inputGateIndex;
	}

	/**
	 * Returns the outputGateIndex.
	 *
	 * @return the outputGateIndex
	 */
	public int getOutputGateIndex() {
		return this.outputGateIndex;
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

	/**
	 * Return the sampling strategy.
	 *
	 * @return the sampling strategy.
	 */
	public SamplingStrategy getSamplingStrategy() {
		return samplingStrategy;
	}

	/**
	 * Returns the memberIndex.
	 *
	 * @return the memberIndex
	 */
	public int getMemberIndex() {
		return this.memberIndex;
	}

	/**
	 * Returns the name.
	 *
	 * @return the name
	 */
	public String getName() {
		return this.name;
	}

	public QosReporterID.Vertex getReporterID() {
		return QosReporterID.forVertex(this.vertexID, this.inputGateID,
				this.outputGateID);
	}

	public QosVertex toQosVertex() {
		return new QosVertex(this.vertexID, this.name, this.reporterInstance,
				this.memberIndex);
	}

	public QosGate toInputGate() {
		return new QosGate(this.inputGateID, this.inputGateIndex);
	}

	public QosGate toOutputGate() {
		return new QosGate(this.outputGateID, this.outputGateIndex);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		this.groupVertexID.write(out);
		this.vertexID.write(out);
		this.reporterInstance.write(out);

		out.writeInt(this.qosManagers.length);
		for (InstanceConnectionInfo qosManager : this.qosManagers) {
			qosManager.write(out);
		}

		out.writeInt(this.inputGateIndex);
		if (this.inputGateIndex != -1) {
			this.inputGateID.write(out);
		}

		out.writeInt(this.outputGateIndex);
		if (this.outputGateIndex != -1) {
			this.outputGateID.write(out);
		}
		
		if (!this.isDummy()) {
			out.writeUTF(samplingStrategy.toString());
		}

		out.writeInt(this.memberIndex);
		out.writeUTF(this.name);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException {
		this.groupVertexID = new JobVertexID();
		this.groupVertexID.read(in);
		this.vertexID = new ExecutionVertexID();
		this.vertexID.read(in);
		this.reporterInstance = new InstanceConnectionInfo();
		this.reporterInstance.read(in);

		this.qosManagers = new InstanceConnectionInfo[in.readInt()];
		for (int i = 0; i < this.qosManagers.length; i++) {
			this.qosManagers[i] = new InstanceConnectionInfo();
			this.qosManagers[i].read(in);
		}
		this.inputGateIndex = in.readInt();
		if (this.inputGateIndex != -1) {
			this.inputGateID = new GateID();
			this.inputGateID.read(in);
		}

		this.outputGateIndex = in.readInt();
		if (this.outputGateIndex != -1) {
			this.outputGateID = new GateID();
			this.outputGateID.read(in);
		}

		if (!this.isDummy()) {
			this.samplingStrategy = SamplingStrategy.valueOf(in.readUTF());
		}
		
		this.memberIndex = in.readInt();
		this.name = in.readUTF();
	}
}
