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
import java.util.LinkedList;

import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.message.AbstractSerializableQosMessage;

/**
 * Informs a task manager about its Qos reporter roles.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class DeployInstanceQosRolesAction extends AbstractSerializableQosMessage implements QosAction {

	private final InstanceConnectionInfo instanceConnectionInfo;

	private final LinkedList<EdgeQosReporterConfig> edgeQosReporters = new LinkedList<EdgeQosReporterConfig>();

	private final LinkedList<VertexQosReporterConfig> vertexQosReporters = new LinkedList<VertexQosReporterConfig>();

	private final LinkedList<CandidateChainConfig> candidateChains = new LinkedList<CandidateChainConfig>();

	public DeployInstanceQosRolesAction() {
		this.instanceConnectionInfo = new InstanceConnectionInfo();
	}

	public DeployInstanceQosRolesAction(JobID jobID,
			InstanceConnectionInfo instanceConnectionInfo) {
		super(jobID);
		this.instanceConnectionInfo = instanceConnectionInfo;
	}

	public InstanceConnectionInfo getInstanceConnectionInfo() {
		return this.instanceConnectionInfo;
	}

	public void addEdgeQosReporter(EdgeQosReporterConfig edgeQosReporter) {
		this.edgeQosReporters.add(edgeQosReporter);
	}

	public void addVertexQosReporter(VertexQosReporterConfig vertexQosReporter) {
		this.vertexQosReporters.add(vertexQosReporter);
	}
	
	public void addCandidateChain(CandidateChainConfig candidateChain) {
		this.candidateChains.add(candidateChain);
	}

	public LinkedList<EdgeQosReporterConfig> getEdgeQosReporters() {
		return this.edgeQosReporters;
	}

	public LinkedList<VertexQosReporterConfig> getVertexQosReporters() {
		return this.vertexQosReporters;
	}

	public LinkedList<CandidateChainConfig> getCandidateChains() {
		return this.candidateChains;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);
		this.instanceConnectionInfo.write(out);
		out.writeInt(this.edgeQosReporters.size());
		for (EdgeQosReporterConfig edgeQosReporter : this.edgeQosReporters) {
			edgeQosReporter.write(out);
		}
		out.writeInt(this.vertexQosReporters.size());
		for (VertexQosReporterConfig vertexQosReporter : this.vertexQosReporters) {
			vertexQosReporter.write(out);
		}
		out.writeInt(this.candidateChains.size());
		for (CandidateChainConfig candidateChain : this.candidateChains) {
			candidateChain.write(out);
		}		
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);
		this.instanceConnectionInfo.read(in);

		int noOfEdgeQosReporters = in.readInt();
		for (int i = 0; i < noOfEdgeQosReporters; i++) {
			EdgeQosReporterConfig edgeQosReporter = new EdgeQosReporterConfig();
			edgeQosReporter.read(in);
			this.edgeQosReporters.add(edgeQosReporter);
		}

		int noOfVertexQosReporters = in.readInt();
		for (int i = 0; i < noOfVertexQosReporters; i++) {
			VertexQosReporterConfig vertexQosReporter = new VertexQosReporterConfig();
			vertexQosReporter.read(in);
			this.vertexQosReporters.add(vertexQosReporter);
		}
		
		int noOfCandidateChains = in.readInt();
		for (int i = 0; i < noOfCandidateChains; i++) {
			CandidateChainConfig candidateChain = new CandidateChainConfig();
			candidateChain.read(in);
			this.candidateChains.add(candidateChain);
		}		
	}
}
