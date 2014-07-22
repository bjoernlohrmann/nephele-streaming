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

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.mockito.Matchers;

import eu.stratosphere.nephele.executiongraph.DistributionPatternProvider;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionSignature;
import eu.stratosphere.nephele.executiongraph.ExecutionStage;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGenericOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.template.GenericInputSplit;
import eu.stratosphere.nephele.types.Record;

/**
 * 
 * Provides fixtures (qos graphs, a job graph and an execution graph). If you
 * use this class in your unit test, be sure to annotate your class with the
 * following Powermock annotations:
 * 
 * @PrepareForTest({ ExecutionSignature.class, AbstractInstance.class,
 *                   AllocatedResource.class })
 * @RunWith(PowerMockRunner.class)
 * 
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class QosGraphFixture {

	public QosGroupVertex vertex1;

	public QosGroupVertex vertex2;

	public QosGroupVertex vertex3;

	public QosGroupVertex vertex4;

	public QosGroupVertex vertex5;

	public QosGroupEdge edge12;

	public QosGroupEdge edge13;

	public QosGroupEdge edge24;

	public QosGroupEdge edge34;

	public QosGroupEdge edge45;

	public QosGroupVertex vertex0;

	public QosGroupVertex vertex1Clone;

	public QosGroupVertex vertex3Clone;

	public QosGroupVertex vertex5Clone;

	public QosGroupVertex vertex6;

	public QosGroupEdge edge05C;

	public QosGroupEdge edge01C;

	public QosGroupEdge edge03C;

	public QosGroupEdge edge5C6;

	public QosGroupVertex vertex10;

	public QosGroupVertex vertex11;

	public QosGroupVertex vertex12;

	public QosGroupVertex vertex13;

	public QosGroupEdge edge1011;

	public QosGroupEdge edge1112;

	public QosGroupEdge edge1213;

	public JobGraph jobGraph;

	public JobInputVertex jobVertex1;

	public JobTaskVertex jobVertex2;

	public JobTaskVertex jobVertex3;

	public JobTaskVertex jobVertex4;

	public JobGenericOutputVertex jobVertex5;

	public ExecutionGroupVertex execVertex1;

	public ExecutionGroupVertex execVertex2;

	public ExecutionGroupVertex execVertex3;

	public ExecutionGroupVertex execVertex4;

	public ExecutionGroupVertex execVertex5;

	public ExecutionGraph execGraph;

	/**
	 * Covers e13,v3,e34,v4,e45
	 */
	public JobGraphLatencyConstraint constraint1;

	/**
	 * Covers e12,v2,e24,v4
	 */
	public JobGraphLatencyConstraint constraint2;

	/**
	 * Covers v2,e24,v4,e45
	 */
	public JobGraphLatencyConstraint constraint3;

	/**
	 * Covers v2,e24,v4
	 */
	public JobGraphLatencyConstraint constraint4;

	/**
	 * Covers e1011,v11,e1112,v12,e1213
	 */
	public JobGraphLatencyConstraint constraint5;

	private InstanceConnectionInfo[][] instanceConnectionInfos;

	public QosGraphFixture() throws Exception {
		// if there is currently no log4j configuration,
		// make a default stdout config with WARN level
		if (!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
			BasicConfigurator.configure();
			Logger.getRootLogger().setLevel(Level.WARN);
		}

		this.makeJobGraph();
		this.makeExecutionGraph();
		this.connectVertices1To5();
		this.connectVertices0To6();
		this.connectvertices10To13();
		this.makeConstraints();
	}

	private void connectvertices10To13() {
		this.vertex10 = new QosGroupVertex(new JobVertexID(), "vertex10");
		this.generateMembers(this.vertex10, 3);

		this.vertex11 = new QosGroupVertex(new JobVertexID(), "vertex11");
		this.generateMembers(this.vertex11, 3);

		this.vertex12 = new QosGroupVertex(new JobVertexID(), "vertex12");
		this.generateMembers(this.vertex12, 3);

		this.vertex13 = new QosGroupVertex(new JobVertexID(), "vertex13");
		this.generateMembers(this.vertex13, 3);

		this.edge1011 = new QosGroupEdge(DistributionPattern.BIPARTITE,
				this.vertex10, this.vertex11, 0, 0);
		this.generateMemberWiring(this.edge1011);

		this.edge1112 = new QosGroupEdge(DistributionPattern.POINTWISE,
				this.vertex11, this.vertex12, 0, 0);
		this.generateMemberWiring(this.edge1112);

		this.edge1213 = new QosGroupEdge(DistributionPattern.BIPARTITE,
				this.vertex12, this.vertex13, 0, 0);
		this.generateMemberWiring(this.edge1213);
	}

	public void makeExecutionGraph() throws Exception {
		mockStatic(ExecutionSignature.class);

		ExecutionSignature execSig = mock(ExecutionSignature.class);

		when(
				ExecutionSignature.createSignature(
						Matchers.eq(AbstractGenericInputTask.class),
						Matchers.any(JobID.class))).thenReturn(execSig);
		when(
				ExecutionSignature.createSignature(
						Matchers.eq(AbstractTask.class),
						Matchers.any(JobID.class))).thenReturn(execSig);
		when(
				ExecutionSignature.createSignature(
						Matchers.eq(AbstractOutputTask.class),
						Matchers.any(JobID.class))).thenReturn(execSig);

		InstanceManager mockInstanceManager = mock(InstanceManager.class);
		InstanceType instType = new InstanceType();
		when(mockInstanceManager.getDefaultInstanceType()).thenReturn(instType);
		this.execGraph = new ExecutionGraph(this.jobGraph, mockInstanceManager);

		// *sigh* this is messy
		ExecutionStage stage = this.execGraph.getStage(0);
		for (int i = 0; i < stage.getNumberOfStageMembers(); i++) {
			ExecutionGroupVertex execVertex = stage.getStageMember(i);
			if (execVertex.getJobVertexID().equals(this.jobVertex1.getID()))
				this.execVertex1 = execVertex;
			else if (execVertex.getJobVertexID()
					.equals(this.jobVertex2.getID()))
				this.execVertex2 = execVertex;
			else if (execVertex.getJobVertexID()
					.equals(this.jobVertex3.getID()))
				this.execVertex3 = execVertex;
			else if (execVertex.getJobVertexID()
					.equals(this.jobVertex4.getID()))
				this.execVertex4 = execVertex;
			else if (execVertex.getJobVertexID()
					.equals(this.jobVertex5.getID()))
				this.execVertex5 = execVertex;
		}

		// inject InstanceConnectionInfos
		this.instanceConnectionInfos = new InstanceConnectionInfo[5][];
		this.instanceConnectionInfos[0] = QosGraphTestUtil
				.generateAndAssignInstances(this.execVertex1);
		this.instanceConnectionInfos[1] = QosGraphTestUtil
				.generateAndAssignInstances(this.execVertex2);
		this.instanceConnectionInfos[2] = QosGraphTestUtil
				.generateAndAssignInstances(this.execVertex3);
		this.instanceConnectionInfos[3] = QosGraphTestUtil
				.generateAndAssignInstances(this.execVertex4);
		this.instanceConnectionInfos[4] = QosGraphTestUtil
				.generateAndAssignInstances(this.execVertex5);
	}

    public void makeConstraints() {
		/**
		 * Covers e13,v3,e34,v4,e45
		 */
		JobGraphSequence sequence1 = new JobGraphSequence();
		sequence1.addEdge(this.jobVertex1.getID(), 1, this.jobVertex3.getID(),
				0);
		sequence1.addVertex(this.jobVertex3.getID(), this.jobVertex3.getName(), 0, 0);
		sequence1.addEdge(this.jobVertex3.getID(), 0, this.jobVertex4.getID(),
				1);
		sequence1.addVertex(this.jobVertex4.getID(), this.jobVertex4.getName(), 1, 0);
		sequence1.addEdge(this.jobVertex4.getID(), 0, this.jobVertex5.getID(),
				0);
		this.constraint1 = new JobGraphLatencyConstraint(sequence1, 2000);

		/**
		 * Covers e12,v2,e24,v4
		 */
		JobGraphSequence sequence2 = new JobGraphSequence();
		sequence2.addEdge(this.jobVertex1.getID(), 0, this.jobVertex2.getID(),
				0);
		sequence2.addVertex(this.jobVertex2.getID(), this.jobVertex2.getName(), 0, 0);
		sequence2.addEdge(this.jobVertex2.getID(), 0, this.jobVertex4.getID(),
				0);
		sequence2.addVertex(this.jobVertex4.getID(), this.jobVertex4.getName(), 0, 0);
		this.constraint2 = new JobGraphLatencyConstraint(sequence2, 2000);

		/**
		 * Covers v2,e24,v4,e45
		 */
		JobGraphSequence sequence3 = new JobGraphSequence();
		sequence3.addVertex(this.jobVertex2.getID(), this.jobVertex2.getName(), 0, 0);
		sequence3.addEdge(this.jobVertex2.getID(), 0, this.jobVertex4.getID(),
				0);
		sequence3.addVertex(this.jobVertex4.getID(), this.jobVertex4.getName(), 0, 0);
		sequence3.addEdge(this.jobVertex4.getID(), 0, this.jobVertex5.getID(),
				0);
		this.constraint3 = new JobGraphLatencyConstraint(sequence3, 2000);

		/**
		 * Covers v2,e24,v4
		 */
		JobGraphSequence sequence4 = new JobGraphSequence();
		sequence4.addVertex(this.jobVertex2.getID(), this.jobVertex2.getName(), 0, 0);
		sequence4.addEdge(this.jobVertex2.getID(), 0, this.jobVertex4.getID(),
				0);
		sequence4.addVertex(this.jobVertex4.getID(), this.jobVertex4.getName(), 0, 0);
		this.constraint4 = new JobGraphLatencyConstraint(sequence4, 2000);

		/**
		 * Covers e1011,v11,e1112,v12,e1213
		 */
		JobGraphSequence sequence5 = new JobGraphSequence();
		sequence5.addEdge(this.vertex10.getJobVertexID(), 0,
				this.vertex11.getJobVertexID(), 0);
		sequence5.addVertex(this.vertex11.getJobVertexID(), this.vertex11.getName(), 0, 0);
		sequence5.addEdge(this.vertex11.getJobVertexID(), 0,
				this.vertex12.getJobVertexID(), 0);
		sequence5.addVertex(this.vertex12.getJobVertexID(), this.vertex12.getName(), 0, 0);
		sequence5.addEdge(this.vertex12.getJobVertexID(), 0,
				this.vertex13.getJobVertexID(), 0);
		this.constraint5 = new JobGraphLatencyConstraint(sequence5, 2000);
	}

	public void makeJobGraph() throws Exception {
		// makes a job graph that contains vertices 1 - 5
		this.jobGraph = new JobGraph();

		this.jobVertex1 = new JobInputVertex("vertex1", this.jobGraph);
		this.jobVertex1.setInputClass(DummyInputTask.class);
		this.jobVertex1.setNumberOfSubtasks(2);
		this.jobVertex2 = new JobTaskVertex("vertex2", this.jobGraph);
		this.jobVertex2.setTaskClass(DummyTask23.class);
		this.jobVertex2.setNumberOfSubtasks(2);
		this.jobVertex3 = new JobTaskVertex("vertex3", this.jobGraph);
		this.jobVertex3.setTaskClass(DummyTask23.class);
		this.jobVertex3.setNumberOfSubtasks(3);
		this.jobVertex4 = new JobTaskVertex("vertex4", this.jobGraph);
		this.jobVertex4.setTaskClass(DummyTask4.class);
		this.jobVertex4.setNumberOfSubtasks(1);
		this.jobVertex5 = new JobGenericOutputVertex("vertex5", this.jobGraph);
		this.jobVertex5.setOutputClass(DummyOutputTask.class);
		this.jobVertex5.setNumberOfSubtasks(5);

		this.jobVertex1.connectTo(this.jobVertex2, ChannelType.NETWORK,
				DistributionPattern.BIPARTITE);
		this.jobVertex1.connectTo(this.jobVertex3, ChannelType.NETWORK,
				DistributionPattern.BIPARTITE);
		this.jobVertex2.connectTo(this.jobVertex4, ChannelType.NETWORK,
				DistributionPattern.POINTWISE);
		this.jobVertex3.connectTo(this.jobVertex4, ChannelType.NETWORK,
				DistributionPattern.POINTWISE);
		this.jobVertex4.connectTo(this.jobVertex5, ChannelType.NETWORK,
				DistributionPattern.BIPARTITE);

		// File jarFile = File.createTempFile("bogus-test", ".jar");
		// JarFileCreator jfc = new JarFileCreator(jarFile);
		// jfc.addClass(AbstractGenericInputTask.class);
		// jfc.addClass(AbstractTask.class);
		// jfc.addClass(AbstractOutputTask.class);
		// jfc.createJarFile();
		// jobGraph.addJar(new Path(jarFile.getAbsolutePath()));
	}

	private void connectVertices0To6() {
		// vertices 0, 1Clone, 3Clone, 5Clone and 6 form a graph to test merging
		this.vertex0 = new QosGroupVertex(new JobVertexID(), "vertex0");
		this.generateMembers(this.vertex0, 15);

		this.vertex1Clone = this.vertex1.cloneWithoutEdges();

		this.vertex3Clone = this.vertex3.cloneWithoutEdges();

		this.vertex5Clone = this.vertex5.cloneWithoutEdges();

		this.vertex6 = new QosGroupVertex(new JobVertexID(), "vertex6");
		this.generateMembers(this.vertex6, 3);

		this.edge05C = new QosGroupEdge(DistributionPattern.BIPARTITE,
				this.vertex0, this.vertex5Clone, 0, 1);
		this.generateMemberWiring(this.edge05C);

		this.edge01C = new QosGroupEdge(DistributionPattern.BIPARTITE,
				this.vertex0, this.vertex1Clone, 1, 0);
		this.generateMemberWiring(this.edge01C);

		this.edge03C = new QosGroupEdge(DistributionPattern.BIPARTITE,
				this.vertex0, this.vertex3Clone, 2, 1);
		this.generateMemberWiring(this.edge03C);

		this.edge5C6 = new QosGroupEdge(DistributionPattern.POINTWISE,
				this.vertex5Clone, this.vertex6, 2, 1);
		this.generateMemberWiring(this.edge5C6);
	}

	private void connectVertices1To5() {
		// vertex 1-5 form a graph. it is identical to the job graph
		this.vertex1 = new QosGroupVertex(this.execVertex1.getJobVertexID(),
				"vertex1");
		this.generateMembers(this.vertex1, this.execVertex1);

		this.vertex2 = new QosGroupVertex(this.execVertex2.getJobVertexID(),
				"vertex2");
		this.generateMembers(this.vertex2, this.execVertex2);

		this.vertex3 = new QosGroupVertex(this.execVertex3.getJobVertexID(),
				"vertex3");
		this.generateMembers(this.vertex3, this.execVertex3);

		this.vertex4 = new QosGroupVertex(this.execVertex4.getJobVertexID(),
				"vertex4");
		this.generateMembers(this.vertex4, this.execVertex4);

		this.vertex5 = new QosGroupVertex(this.execVertex5.getJobVertexID(),
				"vertex5");
		this.generateMembers(this.vertex5, this.execVertex5);

		this.edge12 = new QosGroupEdge(DistributionPattern.BIPARTITE,
				this.vertex1, this.vertex2, 0, 0);
		this.generateMemberWiring(this.edge12);

		this.edge13 = new QosGroupEdge(DistributionPattern.BIPARTITE,
				this.vertex1, this.vertex3, 1, 0);
		this.generateMemberWiring(this.edge13);

		this.edge24 = new QosGroupEdge(DistributionPattern.POINTWISE,
				this.vertex2, this.vertex4, 0, 0);
		this.generateMemberWiring(this.edge24);

		this.edge34 = new QosGroupEdge(DistributionPattern.POINTWISE,
				this.vertex3, this.vertex4, 0, 1);
		this.generateMemberWiring(this.edge34);

		this.edge45 = new QosGroupEdge(DistributionPattern.BIPARTITE,
				this.vertex4, this.vertex5, 0, 0);
		this.generateMemberWiring(this.edge45);
	}

	private void generateMembers(QosGroupVertex groupVertex,
			ExecutionGroupVertex execGroupVertex) {

		for (int i = 0; i < execGroupVertex.getCurrentNumberOfGroupMembers(); i++) {
			ExecutionVertex execVertex = execGroupVertex.getGroupMember(i);
			QosVertex member = new QosVertex(execVertex.getID(),
					execVertex.getName(), execVertex.getAllocatedResource()
							.getInstance().getInstanceConnectionInfo(),
					execVertex.getIndexInVertexGroup());
			groupVertex.setGroupMember(member);
		}
	}

	private void generateMembers(QosGroupVertex vertex, int memberCount) {
		InetAddress address;
		try {
			address = InetAddress.getLocalHost();
			for (int i = 0; i < memberCount; i++) {
				QosVertex member = new QosVertex(new ExecutionVertexID(),
						vertex.getName() + "_" + i, new InstanceConnectionInfo(
								address, 1, 1), i);
				vertex.setGroupMember(member);
			}
		} catch (UnknownHostException e) {
			throw new IllegalStateException(
					"Dummy address could not be generated from local host.", e);
		}
	}

	private void generateMemberWiring(QosGroupEdge groupEdge) {
		int sourceMembers = groupEdge.getSourceVertex().getNumberOfMembers();
		int targetMembers = groupEdge.getTargetVertex().getNumberOfMembers();

		for (int i = 0; i < sourceMembers; i++) {
			QosVertex sourceMember = groupEdge.getSourceVertex().getMember(i);
			QosGate outputGate = new QosGate(new GateID(),
					groupEdge.getOutputGateIndex());
			sourceMember.setOutputGate(outputGate);

			for (int j = 0; j < targetMembers; j++)
				if (DistributionPatternProvider.createWire(
						groupEdge.getDistributionPattern(), i, j,
						sourceMembers, targetMembers)) {

					QosVertex targetMember = groupEdge.getTargetVertex()
							.getMember(j);

					QosGate inputGate = targetMember.getInputGate(groupEdge
							.getInputGateIndex());
					if (inputGate == null) {
						inputGate = new QosGate(new GateID(),
								groupEdge.getOutputGateIndex());
						targetMember.setInputGate(inputGate);
					}

					QosEdge edge = new QosEdge(new ChannelID(),
							new ChannelID(), outputGate.getNumberOfEdges(),
							inputGate.getNumberOfEdges());
					edge.setInputGate(inputGate);
					edge.setOutputGate(outputGate);
				}
		}
	}

	public static class DummyRecord implements Record {
		@Override
		public void write(DataOutput out) throws IOException {
		}

		@Override
		public void read(DataInput in) throws IOException {
		}
	}

	public static class DummyInputTask extends AbstractGenericInputTask {

		@Override
		public void registerInputOutput() {
			new RecordWriter<DummyRecord>(this, DummyRecord.class);
			new RecordWriter<DummyRecord>(this, DummyRecord.class);
		}

		@Override
		public void invoke() throws Exception {
		}

		@Override
		public GenericInputSplit[] computeInputSplits(int foo) {
			return new GenericInputSplit[0];
		}
	}

	public static class DummyOutputTask extends AbstractOutputTask {

		@Override
		public void registerInputOutput() {
			new RecordReader<DummyRecord>(this, DummyRecord.class);
		}

		@Override
		public void invoke() throws Exception {
		}
	}

	public static class DummyTask23 extends AbstractTask {

		@Override
		public void registerInputOutput() {
			new RecordReader<DummyRecord>(this, DummyRecord.class);
			new RecordWriter<DummyRecord>(this, DummyRecord.class);
		}

		@Override
		public void invoke() throws Exception {
		}
	}

	public static class DummyTask4 extends AbstractTask {

		@Override
		public void registerInputOutput() {
			new RecordReader<DummyRecord>(this, DummyRecord.class);
			new RecordReader<DummyRecord>(this, DummyRecord.class);
			new RecordWriter<DummyRecord>(this, DummyRecord.class);
		}

		@Override
		public void invoke() throws Exception {
		}
	}

}
