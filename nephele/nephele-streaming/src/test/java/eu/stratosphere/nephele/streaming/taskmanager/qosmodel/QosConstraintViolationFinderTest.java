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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionSignature;
import eu.stratosphere.nephele.executiongraph.GraphConversionException;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.message.qosreport.EdgeStatistics;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosConstraintViolationFinder;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosConstraintViolationListener;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.SequenceQosSummary;

/**
 * Tests for {@link QosConstraintViolationFinder}
 * 
 * @author Bernd Louis (bernd.louis@gmail.com)
 */
@PrepareForTest({ ExecutionSignature.class, AbstractInstance.class,
		AllocatedResource.class })
@RunWith(PowerMockRunner.class)
public class QosConstraintViolationFinderTest {
	private QosGraphFixture fix;
	@Mock
	private QosConstraintViolationListener listener;
	private EdgeStatistics fauxEdgeStatistics;

	@Before
	public void setUp() throws Exception {
		this.fix = new QosGraphFixture();
		this.fauxEdgeStatistics = new EdgeStatistics(new QosReporterID.Edge(),
				1e7, 500d, 100d, 100d);
	}

	/**
	 * This test is configured to be too fast for
	 * {@link QosGraphFixture#constraint1} and {@link QosGraphFixture#jobGraph}
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testTooFast() throws Exception {
		this.testConstraint1WithLatencies(0.0d, 0.0d, this.listener, true,
				false);
		verify(this.listener, atLeastOnce()).handleViolatedConstraint(any(JobGraphLatencyConstraint.class),
				any(List.class), any(SequenceQosSummary.class));
	}

	/**
	 * This test is configured to be too fast for
	 * {@link QosGraphFixture#constraint1} and {@link QosGraphFixture#jobGraph}
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testTooFastWithStatistics() throws Exception {
		this.testConstraint1WithLatencies(0.0d, 0.0d, this.listener, false,
				true);
		verify(this.listener, atLeastOnce()).handleViolatedConstraint(
				any(JobGraphLatencyConstraint.class),
				any(List.class), any(SequenceQosSummary.class));
	}

	/**
	 * This test is configured to be about right for
	 * {@link QosGraphFixture#constraint1} and {@link QosGraphFixture#jobGraph}
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testAboutRight() throws Exception {
		JobGraphLatencyConstraint c1 = this.fix.constraint1;
		JobGraphSequence sequence = c1.getSequence();

		double d = (double) c1.getLatencyConstraintInMillis()
				/ (sequence.getNumberOfEdges() + sequence.getNumberOfVertices());

		this.testConstraint1WithLatencies(d, d, this.listener, true, false);
		verify(this.listener, never()).handleViolatedConstraint(
				any(JobGraphLatencyConstraint.class),
				any(List.class), any(SequenceQosSummary.class));
	}

	/**
	 * This test is configured to be about right for
	 * {@link QosGraphFixture#constraint1} and {@link QosGraphFixture#jobGraph}
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testAboutRightWithStatistics() throws Exception {
		JobGraphLatencyConstraint c1 = this.fix.constraint1;
		JobGraphSequence sequence = c1.getSequence();

		double d = (double) c1.getLatencyConstraintInMillis()
				/ (sequence.getNumberOfEdges() + sequence.getNumberOfVertices());

		this.testConstraint1WithLatencies(d, d, this.listener, false, true);
		verify(this.listener, never()).handleViolatedConstraint(
				any(JobGraphLatencyConstraint.class),
				any(List.class), any(SequenceQosSummary.class));
	}

	/**
	 * This test is supposed to be *far* too slow for
	 * {@link QosGraphFixture#constraint1} and {@link QosGraphFixture#jobGraph}
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testViolation() throws Exception {
		this.testConstraint1WithLatencies(Double.POSITIVE_INFINITY,
				Double.POSITIVE_INFINITY, this.listener, true, false);
		verify(this.listener, atLeastOnce()).handleViolatedConstraint(
				any(JobGraphLatencyConstraint.class),
				any(List.class), any(SequenceQosSummary.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testVoilationWithStatistics() throws Exception {
		this.testConstraint1WithLatencies(Double.POSITIVE_INFINITY,
				Double.POSITIVE_INFINITY, this.listener, false, true);
		verify(this.listener, atLeastOnce()).handleViolatedConstraint(
				any(JobGraphLatencyConstraint.class),
				any(List.class), any(SequenceQosSummary.class));
	}

	/**
	 * Step 1: Add Qos-Data so the constraint is violated Step 2: Remove some of
	 * the data at the bottleneck vertex4 Step 3: Check for interactions with
	 * the callback ->
	 * {@link QosConstraintViolationListener#handleViolatedConstraint(java.util.List, double)}
	 * -> if there's no interactions we're fine, because
	 * {@link QosConstraintViolationFinder} should have called the callback if
	 * the data was complete and the constraint was violated.
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testIncompleteGraphShouldAbortTraversal() throws Exception {
		QosGraph qg = this.createQosGraph(this.fix.jobGraph,
				this.fix.constraint1);
		Set<QosVertex> startQosVertices = this.getStartQosVertices(qg);
		// Initialize the sequence with positive infinity ..
		this.initializeConstraintWithLatencies(this.fix.constraint1,
				startQosVertices, System.currentTimeMillis(),
				Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, false, true);
		// now let's traverse again and remove all data for vertex4
		for (QosVertex v : startQosVertices)
			new QosGraphTraversal(v, this.fix.constraint1.getSequence(),
					new QosGraphTraversalListener() {
						@Override
						public void processQosVertex(QosVertex vertex,
								SequenceElement<JobVertexID> sequenceElem) {
							if (vertex.getGroupVertex().getName()
									.equals("vertex4"))
								vertex.setQosData(new VertexQosData(vertex));
						}

						@Override
						public void processQosEdge(QosEdge edge,
								SequenceElement<JobVertexID> sequenceElem) {
							if (edge.getOutputGate().getVertex()
									.getGroupVertex().getName()
									.equals("vertex4")
									|| edge.getInputGate().getVertex()
											.getGroupVertex().getName()
											.equals("vertex4"))
								edge.setQosData(new EdgeQosData(edge));
						}
					}).traverseForward();
		new QosConstraintViolationFinder(this.fix.constraint1.getID(), qg,
				this.listener).findSequencesWithViolatedQosConstraint();
		verify(this.listener, never()).handleViolatedConstraint(
				any(JobGraphLatencyConstraint.class),
				any(List.class), any(SequenceQosSummary.class));
	}

	/**
	 * This test tries to remove data from only one member (at vertex 3).
	 * 
	 * Since there is a way from "start to end" this should work (i.e. the
	 * violation finder will report a violation (due to latencies of
	 * {@link Double#POSITIVE_INFINITY}.
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testSlightlyIncompleteGraph() throws Exception {
		QosGraph qosGraph = this.createQosGraph(this.fix.jobGraph,
				this.fix.constraint1);
		Set<QosVertex> startQosVertices = this.getStartQosVertices(qosGraph);
		this.initializeConstraintWithLatencies(this.fix.constraint1,
				startQosVertices, System.currentTimeMillis(),
				Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, false, true);
		// now we're removing
		for (QosVertex v : startQosVertices)
			new QosGraphTraversal(v, this.fix.constraint1.getSequence(),
					new QosGraphTraversalListener() {
						public boolean removed = false;

						@Override
						public void processQosVertex(QosVertex vertex,
								SequenceElement<JobVertexID> sequenceElem) {
							if (!this.removed
									&& vertex.getGroupVertex().getName()
											.equals("vertex3")) {
								// remove exactly one piece of data here ...
								VertexQosData qosData = new VertexQosData(
										vertex);
								// we still have to prepare or this will end in
								// a
								// NullPointerException
								int inputGateIndex = sequenceElem
										.getInputGateIndex();
								int outputGateIndex = sequenceElem
										.getOutputGateIndex();
								qosData.prepareForReportsOnGateCombination(
										inputGateIndex, outputGateIndex);
								qosData.addLatencyMeasurement(inputGateIndex,
										outputGateIndex,
										System.currentTimeMillis(), 400.0d);
								vertex.setQosData(qosData);
								this.removed = true;
							}
						}

						@Override
						public void processQosEdge(QosEdge edge,
								SequenceElement<JobVertexID> sequenceElem) {

						}
					}).traverseForward();
		new QosConstraintViolationFinder(this.fix.constraint1.getID(),
				qosGraph, this.listener)
				.findSequencesWithViolatedQosConstraint();
		verify(this.listener, atLeastOnce()).handleViolatedConstraint(
				any(JobGraphLatencyConstraint.class),
				any(List.class), any(SequenceQosSummary.class));
	}

	/**
	 * TODO: A graph with missing QosData fails with a NullPointerException. Is
	 * that OK?
	 * 
	 * @throws Exception
	 */
	@Test(expected = NullPointerException.class)
	// TODO remove or not
	public void testUninitializedQosGraphViolationFinder() throws Exception {
		QosGraph qosGraph = this.createQosGraph(this.fix.jobGraph,
				this.fix.constraint1);
		QosConstraintViolationFinder qosConstraintViolationFinder = new QosConstraintViolationFinder(
				this.fix.constraint1.getID(), qosGraph, this.listener);
		qosConstraintViolationFinder.findSequencesWithViolatedQosConstraint(); // NullPointerException
	}

	/**
	 * Prepares a latency measurement using {@link QosGraphFixture#constraint1}
	 * and {@link QosGraphFixture#jobGraph}.
	 * 
	 * 
	 * @param vertexLatency
	 *            the latency each {@link QosVertex} is assigned
	 * @param edgeLatency
	 *            latency each {@link QosEdge} is assigned
	 * @param qosConstraintViolationListener
	 *            a usually mocked {@link QosConstraintViolationListener}
	 * @param isEdgeInChain
	 *            set all {@link EdgeQosData#setIsInChain(boolean)} while
	 *            initializing the graph
	 * @param useEdgeStatisticsFixture
	 *            add the {@link #fauxEdgeStatistics} object to each
	 *            {@link EdgeQosData} instance.
	 * @throws GraphConversionException
	 */
	private void testConstraint1WithLatencies(double vertexLatency,
			double edgeLatency,
			QosConstraintViolationListener qosConstraintViolationListener,
			boolean isEdgeInChain, boolean useEdgeStatisticsFixture)
			throws GraphConversionException {
		JobGraphLatencyConstraint constraint1 = this.fix.constraint1;
		QosGraph qg = this.createQosGraph(this.fix.jobGraph, constraint1);
		this.initializeConstraintWithLatencies(constraint1,
				this.getStartQosVertices(qg), System.currentTimeMillis(),
				vertexLatency, edgeLatency, isEdgeInChain,
				useEdgeStatisticsFixture);
		QosConstraintViolationFinder qosConstraintViolationFinder = new QosConstraintViolationFinder(
				constraint1.getID(), qg, qosConstraintViolationListener);
		qosConstraintViolationFinder.findSequencesWithViolatedQosConstraint();
	}

	/**
	 * Initializes a given constraint's vertices and edges with fixed latencies.
	 * 
	 * @param constraint
	 *            the constraint
	 * @param qosStartVertices
	 *            a {@link java.util.Set} of
	 * @param timestamp
	 *            the timestamp value as
	 * @param vertexLatency
	 *            the latency assigned to vertices
	 * @param edgeLatency
	 *            the latency assigned to edges
	 * @param edgeIsInChain
	 *            set all {@link EdgeQosData#setIsInChain(boolean)} while
	 *            initializing the graph
	 * @param useEdgeStatisticsFixture
	 *            add the {@link #fauxEdgeStatistics} object to each
	 *            {@link EdgeQosData} instance.
	 */
	private void initializeConstraintWithLatencies(
			JobGraphLatencyConstraint constraint,
			Set<QosVertex> qosStartVertices, final long timestamp,
			final double vertexLatency, final double edgeLatency,
			final boolean edgeIsInChain, final boolean useEdgeStatisticsFixture) {
		for (QosVertex startVertex : qosStartVertices) {
			QosGraphTraversal qgt = new QosGraphTraversal(startVertex,
					constraint.getSequence(), new QosGraphTraversalListener() {
						@Override
						public void processQosVertex(QosVertex vertex,
								SequenceElement<JobVertexID> sequenceElem) {
							if (vertex.getQosData() != null)
								return;
							VertexQosData vertexQosData = new VertexQosData(
									vertex);
							int inputGateIndex = sequenceElem
									.getInputGateIndex();
							int outputGateIndex = sequenceElem
									.getOutputGateIndex();
							vertexQosData.prepareForReportsOnGateCombination(
									inputGateIndex, outputGateIndex);
							vertexQosData.addLatencyMeasurement(inputGateIndex,
									outputGateIndex, timestamp, vertexLatency);
							vertex.setQosData(vertexQosData);
						}

						@Override
						public void processQosEdge(QosEdge edge,
								SequenceElement<JobVertexID> sequenceElem) {
							if (edge.getQosData() != null)
								return;
							EdgeQosData edgeQosData = new EdgeQosData(edge);
							edgeQosData.addLatencyMeasurement(timestamp,
									edgeLatency);
							edgeQosData.setIsInChain(edgeIsInChain);
							if (useEdgeStatisticsFixture)
								edgeQosData
										.addOutputChannelStatisticsMeasurement(
												timestamp,
												QosConstraintViolationFinderTest.this.fauxEdgeStatistics);
							edge.setQosData(edgeQosData);
						}
					});
			qgt.traverseForward();
		}
	}

	/**
	 * Maps start vertices on a graph to their member level.
	 * 
	 * @param qg
	 *            the graph
	 * @return a Set containing the individual members of the start vertices.
	 */
	private Set<QosVertex> getStartQosVertices(QosGraph qg) {
		Set<QosGroupVertex> startVertices = qg.getStartVertices();
		Set<QosVertex> qosStartVertices = new HashSet<QosVertex>();
		for (QosGroupVertex qgv : startVertices)
			// qosStartVertices.addAll(qgv.getMembers());
			for (QosVertex qv : qgv.getMembers())
				qosStartVertices.add(qv);
		return qosStartVertices;
	}

	private QosGraph createQosGraph(JobGraph jg,
			JobGraphLatencyConstraint constraint)
			throws GraphConversionException {
		InstanceType instanceType = new InstanceType();
		InstanceManager instanceManager = mock(InstanceManager.class);
		when(instanceManager.getDefaultInstanceType()).thenReturn(instanceType);
		ExecutionGraph eg = new ExecutionGraph(jg, instanceManager);
		return QosGraphFactory.createConstrainedQosGraph(eg, constraint);
	}
}
