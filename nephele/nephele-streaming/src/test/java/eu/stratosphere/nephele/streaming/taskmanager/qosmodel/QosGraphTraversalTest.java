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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionSignature;
import eu.stratosphere.nephele.executiongraph.GraphConversionException;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGenericOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphFixture.DummyRecord;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.AbstractTask;

/**
 * Tests for {@link QosGraphTraversal}
 * 
 * @author Bernd Louis
 * 
 */
@PrepareForTest({ ExecutionSignature.class, AbstractInstance.class,
		AllocatedResource.class })
@RunWith(PowerMockRunner.class)
public class QosGraphTraversalTest {

	private QosGraphFixture fixture;
	private JobInputVertex input;
	private JobTaskVertex task1;
	private JobTaskVertex task2;
	private JobGenericOutputVertex output;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		this.fixture = new QosGraphFixture();
	}

	/**
	 * Tests fixture constraint / sequence 1 forward
	 * 
	 * Covers e13,v3,e34,v4,e45
	 * 
	 * @throws Exception
	 */
	@Test
	public void testFixtureSequence1() throws Exception {
		QosGraph qg = this.createQosGraph(this.fixture.jobGraph,
				this.fixture.constraint1);
		QosVertex qosv = this.getStartVertex(qg);
		JobGraphSequence sequence = this.fixture.constraint1.getSequence();
		QosGraphTraversalListener listener = mock(QosGraphTraversalListener.class);
		QosGraphTraversal tr = new QosGraphTraversal(qosv, sequence, listener);
		tr.traverseForward();
		verify(listener, times(4)).processQosVertex(any(QosVertex.class),
				any(SequenceElement.class));
		verify(listener, times(11)).processQosEdge(any(QosEdge.class),
				any(SequenceElement.class));
	}

	/**
	 * Tests fixture constraint / sequence 1 forward
	 * 
	 * Covers e13,v3,e34,v4,e45
	 * 
	 * Traverse Once is off, so there's more vertices and edges to traverse
	 * 
	 * @throws Exception
	 */
	@Test
	public void testFixtureSequence1TraverseMultiple() throws Exception {
		QosGraph qg = this.createQosGraph(this.fixture.jobGraph,
				this.fixture.constraint1);
		QosVertex qosv = this.getStartVertex(qg);
		JobGraphSequence sequence = this.fixture.constraint1.getSequence();
		QosGraphTraversalListener listener = mock(QosGraphTraversalListener.class);
		QosGraphTraversal tr = new QosGraphTraversal(qosv, sequence, listener);
		tr.traverseForward(true, false);
		verify(listener, times(6)).processQosVertex(any(QosVertex.class),
				any(SequenceElement.class));
		verify(listener, times(21)).processQosEdge(any(QosEdge.class),
				any(SequenceElement.class));
	}

	/**
	 * Tests fixture constraint / sequence 1 backward
	 * 
	 * @throws Exception
	 */
	@Test
	public void testFixtureSequence1Backward() throws Exception {
		QosGraph qg = this.createQosGraph(this.fixture.jobGraph,
				this.fixture.constraint1);
		QosVertex qosv = this.getEndVertex(qg);
		JobGraphSequence sequence = this.fixture.constraint1.getSequence();
		QosGraphTraversalListener listener = mock(QosGraphTraversalListener.class);
		QosGraphTraversal tr = new QosGraphTraversal(qosv, sequence, listener);
		tr.traverseBackward();
		verify(listener, times(4)).processQosVertex(any(QosVertex.class),
				any(SequenceElement.class));
		verify(listener, times(10)).processQosEdge(any(QosEdge.class),
				any(SequenceElement.class));
	}

	/**
	 * Tests fixture constraint / sequence 1 backward
	 * 
	 * @throws Exception
	 */
	@Test
	public void testFixtureSequence1BackwardSkipStartTraverseMultiple()
			throws Exception {
		QosGraph qg = this.createQosGraph(this.fixture.jobGraph,
				this.fixture.constraint1);
		QosVertex qosv = this.getEndVertex(qg);
		JobGraphSequence sequence = this.fixture.constraint1.getSequence();
		QosGraphTraversalListener listener = mock(QosGraphTraversalListener.class);
		QosGraphTraversal tr = new QosGraphTraversal(qosv, sequence, listener);
		tr.traverseBackward(false, false);
		verify(listener, times(4)).processQosVertex(any(QosVertex.class),
				any(SequenceElement.class));
		verify(listener, times(10)).processQosEdge(any(QosEdge.class),
				any(SequenceElement.class));
	}

	/**
	 * Tests fixture constraint / sequence 2 forward
	 * 
	 * Covers e12,v2,e24,v4
	 * 
	 * @throws Exception
	 */
	@Test
	public void testFixtureSequence2() throws Exception {
		QosGraph qg = this.createQosGraph(this.fixture.jobGraph,
				this.fixture.constraint2);
		QosVertex qosv = this.getStartVertex(qg);
		JobGraphSequence sequence = this.fixture.constraint2.getSequence();
		QosGraphTraversalListener listener = mock(QosGraphTraversalListener.class);
		QosGraphTraversal tr = new QosGraphTraversal(qosv, sequence, listener);
		tr.traverseForward();
		verify(listener, times(3)).processQosVertex(any(QosVertex.class),
				any(SequenceElement.class));
		verify(listener, times(4)).processQosEdge(any(QosEdge.class),
				any(SequenceElement.class));
	}

	/**
	 * Tests fixture constraint / sequence 2 forward
	 * 
	 * Covers e12,v2,e24,v4
	 * 
	 * This tests skips the start vertex which should have no effect because the
	 * sequence starts with an edge.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testFixtureSequence2SkipStart() throws Exception {
		QosGraph qg = this.createQosGraph(this.fixture.jobGraph,
				this.fixture.constraint2);
		QosVertex qosv = this.getStartVertex(qg);
		JobGraphSequence sequence = this.fixture.constraint2.getSequence();
		QosGraphTraversalListener listener = mock(QosGraphTraversalListener.class);
		QosGraphTraversal tr = new QosGraphTraversal(qosv, sequence, listener);
		tr.traverseForward(false, true);
		verify(listener, times(3)).processQosVertex(any(QosVertex.class),
				any(SequenceElement.class));
		verify(listener, times(4)).processQosEdge(any(QosEdge.class),
				any(SequenceElement.class));
	}

	/**
	 * Tests fixture constraint / sequence 2 backward
	 * 
	 * @throws Exception
	 */
	@Test
	public void testFixtureSequence2Backward() throws Exception {
		QosGraph qg = this.createQosGraph(this.fixture.jobGraph,
				this.fixture.constraint2);
		QosVertex qosv = this.getEndVertex(qg);
		JobGraphSequence sequence = this.fixture.constraint2.getSequence();
		QosGraphTraversalListener listener = mock(QosGraphTraversalListener.class);
		QosGraphTraversal tr = new QosGraphTraversal(qosv, sequence, listener);
		tr.traverseBackward();
		verify(listener, times(3)).processQosVertex(any(QosVertex.class),
				any(SequenceElement.class));
		verify(listener, times(6)).processQosEdge(any(QosEdge.class),
				any(SequenceElement.class));
	}

	/**
	 * Tests fixture constraint / sequence 3
	 * 
	 * Covers v2,e24,v4,e45
	 * 
	 * @throws Exception
	 */
	@Test
	public void testFixtureSequence3() throws Exception {
		QosGraph qg = this.createQosGraph(this.fixture.jobGraph,
				this.fixture.constraint3);
		QosVertex qosv = this.getStartVertex(qg);
		JobGraphSequence sequence = this.fixture.constraint3.getSequence();
		QosGraphTraversalListener listener = mock(QosGraphTraversalListener.class);
		QosGraphTraversal tr = new QosGraphTraversal(qosv, sequence, listener);
		tr.traverseForward();
		verify(listener, times(2)).processQosVertex(any(QosVertex.class),
				any(SequenceElement.class));
		verify(listener, times(6)).processQosEdge(any(QosEdge.class),
				any(SequenceElement.class));
	}

	/**
	 * Tests fixture constraint / sequence 3 backward
	 * 
	 * Covers v2,e24,v4,e45
	 * 
	 * @throws Exception
	 */
	@Test
	public void testFixtureSequence3Backward() throws Exception {
		QosGraph qg = this.createQosGraph(this.fixture.jobGraph,
				this.fixture.constraint3);
		QosVertex qosv = this.getEndVertex(qg);
		JobGraphSequence sequence = this.fixture.constraint3.getSequence();
		QosGraphTraversalListener listener = mock(QosGraphTraversalListener.class);
		QosGraphTraversal tr = new QosGraphTraversal(qosv, sequence, listener);
		tr.traverseBackward();
		verify(listener, times(3)).processQosVertex(any(QosVertex.class),
				any(SequenceElement.class));
		verify(listener, times(3)).processQosEdge(any(QosEdge.class),
				any(SequenceElement.class));
	}

	/**
	 * Tests fixture constraint / sequence 3 like
	 * {@link QosGraphTraversalTest#testFixtureSequence3()}
	 * 
	 * The start node should not be traversed.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testFixtureSequence3SkipStart() throws Exception {
		QosGraph qg = this.createQosGraph(this.fixture.jobGraph,
				this.fixture.constraint3);
		QosVertex qosv = this.getStartVertex(qg);
		JobGraphSequence sequence = this.fixture.constraint3.getSequence();
		QosGraphTraversalListener listener = mock(QosGraphTraversalListener.class);
		QosGraphTraversal tr = new QosGraphTraversal(qosv, sequence, listener);
		tr.traverseForward(false, true);
		verify(listener, times(1)).processQosVertex(any(QosVertex.class),
				any(SequenceElement.class));
		verify(listener, times(6)).processQosEdge(any(QosEdge.class),
				any(SequenceElement.class));
	}

	/**
	 * Tests fixture constraint / sequence 3 like
	 * {@link QosGraphTraversalTest#testFixtureSequence3()}
	 * 
	 * Traverse once
	 * 
	 * @throws Exception
	 */
	@Test
	public void testFixtureSequence3TraverseOnce() throws Exception {
		QosGraph qg = this.createQosGraph(this.fixture.jobGraph,
				this.fixture.constraint3);
		QosVertex qosv = this.getStartVertex(qg);
		JobGraphSequence sequence = this.fixture.constraint3.getSequence();
		QosGraphTraversalListener listener = mock(QosGraphTraversalListener.class);
		QosGraphTraversal tr = new QosGraphTraversal(qosv, sequence, listener);
		tr.traverseForward(true, false);
		verify(listener, times(2)).processQosVertex(any(QosVertex.class),
				any(SequenceElement.class));
		verify(listener, times(6)).processQosEdge(any(QosEdge.class),
				any(SequenceElement.class));
	}

	/**
	 * Tests fixture constraint / sequence 4
	 * 
	 * v2,e24,v4
	 * 
	 * @throws Exception
	 */
	@Test
	public void testFixtureSequence4() throws Exception {
		QosGraph qg = this.createQosGraph(this.fixture.jobGraph,
				this.fixture.constraint4);
		QosVertex qosv = this.getStartVertex(qg);
		JobGraphSequence sequence = this.fixture.constraint4.getSequence();
		QosGraphTraversalListener listener = mock(QosGraphTraversalListener.class);
		QosGraphTraversal tr = new QosGraphTraversal(qosv, sequence, listener);
		tr.traverseForward();
		verify(listener, times(2)).processQosVertex(any(QosVertex.class),
				any(SequenceElement.class));
		verify(listener, times(1)).processQosEdge(any(QosEdge.class),
				any(SequenceElement.class));
	}

	/**
	 * Tests fixture constraint / sequence 4 backward
	 * 
	 * v2,e24,v4
	 * 
	 * @throws Exception
	 */
	@Test
	public void testFixtureSequence4Backward() throws Exception {
		QosGraph qg = this.createQosGraph(this.fixture.jobGraph,
				this.fixture.constraint4);
		QosVertex qosv = this.getEndVertex(qg);
		JobGraphSequence sequence = this.fixture.constraint4.getSequence();
		QosGraphTraversalListener listener = mock(QosGraphTraversalListener.class);
		QosGraphTraversal tr = new QosGraphTraversal(qosv, sequence, listener);
		tr.traverseBackward();
		verify(listener, times(3)).processQosVertex(any(QosVertex.class),
				any(SequenceElement.class));
		verify(listener, times(2)).processQosEdge(any(QosEdge.class),
				any(SequenceElement.class));
	}

	/**
	 * Tests fixture constraint / sequence 4
	 * 
	 * like {@link QosGraphTraversalTest#testFixtureSequence4()} but without
	 * start vertex
	 * 
	 * v2,e24,v4
	 * 
	 * @throws Exception
	 */
	@Test
	public void testFixtureSequence4SkipStart() throws Exception {
		QosGraph qg = this.createQosGraph(this.fixture.jobGraph,
				this.fixture.constraint4);
		QosVertex qosv = this.getStartVertex(qg);
		JobGraphSequence sequence = this.fixture.constraint4.getSequence();
		QosGraphTraversalListener listener = mock(QosGraphTraversalListener.class);
		QosGraphTraversal tr = new QosGraphTraversal(qosv, sequence, listener);
		tr.traverseForward(false, true);
		verify(listener, times(1)).processQosVertex(any(QosVertex.class),
				any(SequenceElement.class));
		verify(listener, times(1)).processQosEdge(any(QosEdge.class),
				any(SequenceElement.class));
	}

	/**
	 * returns the first (index=0) start (origin) vertex of a given
	 * {@link QosGraph}
	 * 
	 * @param qg
	 *            A {@link QosGraph}
	 * @return the first {@link QosVertex} if the start vertices of that graph
	 */
	private QosVertex getStartVertex(QosGraph qg) {
		return qg.getStartVertices().iterator().next().getMember(0);
	}

	/**
	 * returns the first (index=0) end (sink) vertex of a given {@link QosGraph}
	 * 
	 * @param qg
	 *            A {@link QosGraph}
	 * @return the first {@link QosVertex} if the start vertices of that graph
	 */
	private QosVertex getEndVertex(QosGraph qg) {
		return qg.getEndVertices().iterator().next().getMember(0);
	}

	/**
	 * Creates a {@link QosGraph} from a {@link JobGraph} and a
	 * {@link JobGraphLatencyConstraint}
	 * 
	 * @param jg
	 *            the base {@link JobGraph}
	 * @param constraint
	 *            the {@link JobGraphLatencyConstraint}
	 * @return the resulting {@link QosGraph}
	 * @throws GraphConversionException
	 *             if no {@link ExecutionGraph} could be created from the job
	 *             graph
	 */
	private QosGraph createQosGraph(JobGraph jg,
			JobGraphLatencyConstraint constraint)
			throws GraphConversionException {
		InstanceType instanceType = new InstanceType();
		InstanceManager instanceManager = mock(InstanceManager.class);
		when(instanceManager.getDefaultInstanceType()).thenReturn(instanceType);
		ExecutionGraph eg = new ExecutionGraph(jg, instanceManager);
		return QosGraphFactory.createConstrainedQosGraph(eg, constraint);
	}

	/**
	 * Traverses the very simple {@link JobGraph} from
	 * {@link QosGraphTraversalTest#createSimpleJobGraph()}
	 * 
	 * @throws JobGraphDefinitionException
	 * @throws GraphConversionException
	 */
	@Test
	public void testSimpleGraph() throws JobGraphDefinitionException,
			GraphConversionException {
		JobGraph jobGraph = this.createSimpleJobGraph();
		JobGraphSequence seq = new JobGraphSequence();
		seq.addEdge(this.input.getID(), 0, this.task1.getID(), 0);
		seq.addVertex(this.task1.getID(), this.task1.getName(), 0, 0);
		seq.addEdge(this.task1.getID(), 0, this.task2.getID(), 0);
		seq.addVertex(this.task2.getID(), this.task2.getName(), 0, 0);
		seq.addEdge(this.task2.getID(), 0, this.output.getID(), 0);
		JobGraphLatencyConstraint constraint = new JobGraphLatencyConstraint(
				seq, 2000);

		QosGraph qosGraph = this.createQosGraph(jobGraph, constraint);
		Set<QosGroupVertex> startVertices = qosGraph.getStartVertices();
		assertEquals(1, startVertices.size());
		QosGroupVertex qosStartGroupVertex = startVertices.iterator().next();
		assertEquals(2, qosStartGroupVertex.getNumberOfMembers());
		QosVertex qosStartVertex = qosStartGroupVertex.getMember(0);
		assertEquals("inputVertex0", qosStartVertex.getName());

		QosGraphTraversalListener listener = mock(QosGraphTraversalListener.class);
		QosGraphTraversal traversal = new QosGraphTraversal(qosStartVertex,
				constraint.getSequence(), listener);

		traversal.traverseForward();
		verify(listener, times(5)).processQosEdge(any(QosEdge.class),
				any(SequenceElement.class));
		// visiting task taskVertex10 taskVertex20 and 21
		// at this stage is not visiting input and output vertices ...
		verify(listener, times(3)).processQosVertex(any(QosVertex.class),
				any(SequenceElement.class));
	}

	/**
	 * Creates a very simple graph of the form
	 * 
	 * I >(P) T1 >(B) T2 >(P) O
	 * 
	 * @return the {@link JobGraph}
	 * @throws JobGraphDefinitionException
	 */
	private JobGraph createSimpleJobGraph() throws JobGraphDefinitionException {
		JobGraph jg = new JobGraph();

		this.input = new JobInputVertex("inputVertex", jg);
		this.input.setInputClass(SimpleInputTask.class);
		this.input.setNumberOfSubtasks(2);

		this.task1 = new JobTaskVertex("taskVertex1", jg);
		this.task1.setTaskClass(SimpleTask.class);
		this.task1.setNumberOfSubtasks(2);

		this.task2 = new JobTaskVertex("taskVertex2", jg);
		this.task2.setTaskClass(SimpleTask.class);
		this.task2.setNumberOfSubtasks(2);

		this.output = new JobGenericOutputVertex("output", jg);
		this.output.setOutputClass(SimpleOutputTask.class);
		this.output.setNumberOfSubtasks(2);

		this.input.connectTo(this.task1, ChannelType.NETWORK,
				DistributionPattern.POINTWISE);
		this.task1.connectTo(this.task2, ChannelType.NETWORK,
				DistributionPattern.BIPARTITE);
		this.task2.connectTo(this.output, ChannelType.NETWORK,
				DistributionPattern.POINTWISE);
		return jg;
	}

	private static class SimpleInputTask extends AbstractGenericInputTask {

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.nephele.template.AbstractInvokable#registerInputOutput
		 * ()
		 */
		@Override
		public void registerInputOutput() {
			new RecordWriter<DummyRecord>(this, DummyRecord.class);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see eu.stratosphere.nephele.template.AbstractInvokable#invoke()
		 */
		@Override
		public void invoke() throws Exception {
			// does nothing
		}

	}

	private static class SimpleTask extends AbstractTask {

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.nephele.template.AbstractInvokable#registerInputOutput
		 * ()
		 */
		@Override
		public void registerInputOutput() {
			new RecordReader<DummyRecord>(this, DummyRecord.class);
			new RecordWriter<DummyRecord>(this, DummyRecord.class);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see eu.stratosphere.nephele.template.AbstractInvokable#invoke()
		 */
		@Override
		public void invoke() throws Exception {
		}
	}

	private static class SimpleOutputTask extends AbstractOutputTask {

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.nephele.template.AbstractInvokable#registerInputOutput
		 * ()
		 */
		@Override
		public void registerInputOutput() {
			new RecordReader<DummyRecord>(this, DummyRecord.class);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see eu.stratosphere.nephele.template.AbstractInvokable#invoke()
		 */
		@Override
		public void invoke() throws Exception {
		}
	}
}
