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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.JobGraphSequence;

/**
 * @author Bjoern Lohrmann
 * 
 */
@PrepareForTest({ ExecutionSignature.class, AbstractInstance.class,
		AllocatedResource.class })
@RunWith(PowerMockRunner.class)
public class QosGraphTest {

	private QosGraphFixture fix;
	private Map<String, Integer> vertexCardinality;

	@Before
	public void setup() throws Exception {
		this.fix = new QosGraphFixture();

		this.vertexCardinality = new HashMap<String, Integer>();
		this.vertexCardinality.put(this.fix.jobVertex1.getName(),
				this.fix.jobVertex1.getNumberOfSubtasks());
		this.vertexCardinality.put(this.fix.jobVertex2.getName(),
				this.fix.jobVertex2.getNumberOfSubtasks());
		this.vertexCardinality.put(this.fix.jobVertex3.getName(),
				this.fix.jobVertex3.getNumberOfSubtasks());
		this.vertexCardinality.put(this.fix.jobVertex4.getName(),
				this.fix.jobVertex4.getNumberOfSubtasks());
		this.vertexCardinality.put(this.fix.jobVertex5.getName(),
				this.fix.jobVertex5.getNumberOfSubtasks());
	}

	@Test
	public void testConstructorWithStartVertex() {
		QosGraph graph = new QosGraph(this.fix.vertex1);
		QosGraphTestUtil.assertQosGraphIdenticalToFixture1To5(graph, this.fix);
	}

	@Test
	public void testMergeForwardWithEmptyGraph() {
		QosGraph graph = new QosGraph();
		graph.mergeForwardReachableGroupVertices(this.fix.vertex1);
		QosGraphTestUtil.assertQosGraphEqualToFixture1To5(graph, this.fix);
	}

	@Test
	public void testMergeForwardWithNonemptyGraph() {
		QosGraph graph = new QosGraph(this.fix.vertex1);
		graph.mergeForwardReachableGroupVertices(this.fix.vertex0);
		this.assertMergedFixtureGraphs(graph);
	}

	/**
	 * @param graph
	 */
	private void assertMergedFixtureGraphs(QosGraph graph) {
		assertEquals(7, graph.getNumberOfVertices());
		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex0,
				graph);
		QosGraphTestUtil.assertContainsIdentical(this.fix.vertex1, graph);
		QosGraphTestUtil.assertContainsIdentical(this.fix.vertex2, graph);
		QosGraphTestUtil.assertContainsIdentical(this.fix.vertex3, graph);
		QosGraphTestUtil.assertContainsIdentical(this.fix.vertex4, graph);
		QosGraphTestUtil.assertContainsIdentical(this.fix.vertex5, graph);
		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex6,
				graph);

		assertEquals(1,
				graph.getGroupVertexByID(this.fix.vertex1.getJobVertexID())
						.getNumberOfInputGates());
		assertEquals(2,
				graph.getGroupVertexByID(this.fix.vertex3.getJobVertexID())
						.getNumberOfInputGates());
		assertEquals(2,
				graph.getGroupVertexByID(this.fix.vertex5.getJobVertexID())
						.getNumberOfInputGates());
		assertEquals(1,
				graph.getGroupVertexByID(this.fix.vertex5.getJobVertexID())
						.getNumberOfOutputGates());

		assertEquals(1, graph.getStartVertices().size());
		assertEquals(this.fix.vertex0, graph.getStartVertices().iterator()
				.next());
		assertEquals(1, graph.getEndVertices().size());
		assertEquals(this.fix.vertex6, graph.getEndVertices().iterator().next());
	}

	@Test
	public void testMergeBackwardEmptyGraph() {
		QosGraph graph = new QosGraph();
		graph.mergeBackwardReachableGroupVertices(this.fix.vertex5);
		QosGraphTestUtil.assertQosGraphEqualToFixture1To5(graph, this.fix);
	}

	@Test
	public void testMergeBackwardWithNonemptyGraph() {
		QosGraph graph = new QosGraph(this.fix.vertex1);
		graph.mergeBackwardReachableGroupVertices(this.fix.vertex6);

		assertEquals(7, graph.getNumberOfVertices());
		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex0,
				graph);
		QosGraphTestUtil.assertContainsIdentical(this.fix.vertex1, graph);
		QosGraphTestUtil.assertContainsIdentical(this.fix.vertex2, graph);
		QosGraphTestUtil.assertContainsIdentical(this.fix.vertex3, graph);
		QosGraphTestUtil.assertContainsIdentical(this.fix.vertex4, graph);
		QosGraphTestUtil.assertContainsIdentical(this.fix.vertex5, graph);
		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex6,
				graph);

		assertEquals(1,
				graph.getGroupVertexByID(this.fix.vertex0.getJobVertexID())
						.getNumberOfOutputGates());
		assertEquals(0,
				graph.getGroupVertexByID(this.fix.vertex1.getJobVertexID())
						.getNumberOfInputGates());
		assertEquals(1,
				graph.getGroupVertexByID(this.fix.vertex3.getJobVertexID())
						.getNumberOfInputGates());
		assertEquals(2,
				graph.getGroupVertexByID(this.fix.vertex5.getJobVertexID())
						.getNumberOfInputGates());
		assertEquals(1,
				graph.getGroupVertexByID(this.fix.vertex5.getJobVertexID())
						.getNumberOfOutputGates());

		assertEquals(2, graph.getStartVertices().size());
		assertTrue(graph.getStartVertices().contains(this.fix.vertex0));
		assertTrue(graph.getStartVertices().contains(this.fix.vertex1));
		assertEquals(1, graph.getEndVertices().size());
		assertEquals(this.fix.vertex6, graph.getEndVertices().iterator().next());
	}

	@Test
	public void testMergeIntoEmptyGraph() {
		QosGraph graph = new QosGraph();
		graph.merge(new QosGraph(this.fix.vertex1));
		QosGraphTestUtil.assertQosGraphEqualToFixture1To5(graph, this.fix);
	}

	@Test
	public void testMergeEmptyIntoNonEmptyGraph() {
		QosGraph graph = new QosGraph(this.fix.vertex1);
		graph.merge(new QosGraph());
		QosGraphTestUtil.assertQosGraphIdenticalToFixture1To5(graph, this.fix);
	}

	@Test
	public void testMergeNonEmptyIntoNonEmptyGraph() {
		QosGraph graph = new QosGraph(this.fix.vertex1);
		graph.merge(new QosGraph(this.fix.vertex0));
		this.assertMergedFixtureGraphs(graph);
	}

	@Test
	public void testCloneWithoutMembers() {
		QosGraph orig = new QosGraph(this.fix.vertex10);

		QosGraph clone = orig.cloneWithoutMembers();
		assertEquals(4, clone.getNumberOfVertices());

		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex10,
				clone, false);
		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex11,
				clone, false);
		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex12,
				clone, false);
		QosGraphTestUtil.assertContainsEqualButNotIdentical(this.fix.vertex13,
				clone, false);

		assertEquals(1, clone.getStartVertices().size());
		assertEquals(this.fix.vertex10, clone.getStartVertices().iterator()
				.next());
		assertEquals(1, clone.getEndVertices().size());
		assertEquals(this.fix.vertex13, clone.getEndVertices().iterator()
				.next());
	}

	@Test
	public void testSimpleQosGraphMergeVertices() throws Exception {
		final List<String> expectedVerticesNames = Arrays.asList("vertex1",
				"vertex2", "vertex3", "vertex4", "vertex5");
		// e13, v3, e34, v4, e45
		// let's try and assert that we found anything
		// we're expecting all vertices to be in this graph
		QosGraph graph1 = this.createQosGraphWithConstraint(this.fix.jobGraph,
				this.fix.constraint1);
		graph1.merge(this.createQosGraphWithConstraint(this.fix.jobGraph,
				this.fix.constraint2));
		Set<String> expectedVertexNames = new HashSet<String>(
				expectedVerticesNames); // the vertices we expect to be in the
										// graph
		for (QosGroupVertex v : graph1.getAllVertices()) {
			expectedVertexNames.remove(v.getName());
			assertEquals(this.vertexCardinality.get(v.getName()),
					Integer.valueOf(v.getNumberOfMembers()));
		}
		assertTrue(String.format(
				"Not all expected vertices were encountered. Left %d (%s)",
				expectedVertexNames.size(), join(expectedVertexNames)),
				expectedVertexNames.isEmpty());

		// let's try the other way around
		QosGraph graph2 = this.createQosGraphWithConstraint(this.fix.jobGraph,
				this.fix.constraint2);
		graph2.merge(this.createQosGraphWithConstraint(this.fix.jobGraph,
				this.fix.constraint1));
		expectedVertexNames = new HashSet<String>(expectedVerticesNames);
		for (QosGroupVertex v : graph2.getAllVertices()) {
			expectedVertexNames.remove(v.getName());
			assertEquals(this.vertexCardinality.get(v.getName()),
					Integer.valueOf(v.getNumberOfMembers()));
		}
		assertTrue(String.format(
				"Not all expected vertices were encountered. Left %d (%s)",
				expectedVertexNames.size(), join(expectedVertexNames)),
				expectedVertexNames.isEmpty());

	}

	/**
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSimpleQosGraphMergeEdgesAndGates() throws Exception {
		QosGraph graph1 = this.createQosGraphWithConstraint(this.fix.jobGraph,
				this.fix.constraint1);
		graph1.merge(this.createQosGraphWithConstraint(this.fix.jobGraph,
				this.fix.constraint2));
		// let's check if we've got the right amount of edges here
		Set<QosGroupVertex> startVertices = graph1.getStartVertices();
		assertEquals("There should be only one start vertex", 1,
				startVertices.size());
		QosGroupVertex v1 = startVertices.iterator().next();
		assertEquals("vertex1 should have 2 output gates", 2,
				v1.getNumberOfOutputGates());
		assertEquals("vertex1 should have no input gates", 0,
				v1.getNumberOfInputGates());
		assertEquals("The Start-Vertex should be vertex1", "vertex1",
				v1.getName());
		assertEquals(0, v1.getNumberOfInputGates());
		Set<QosGroupVertex> supposedSplitVertices = new HashSet<QosGroupVertex>(Arrays.asList(
				this.fix.vertex2, this.fix.vertex3));
		for (QosGroupEdge ge : v1.getForwardEdges()) {
			QosGroupVertex targetVertex = ge.getTargetVertex();
			assertEquals("Source vertex should be vertex1", v1,
					ge.getSourceVertex());
			assertTrue(supposedSplitVertices.contains(targetVertex));
		}
		// member level
		assertEquals("There should be 2 members for v1", 2,
				v1.getNumberOfMembers());
		for (QosVertex memberV1 : v1.getMembers()) {
			memberV1.getOutputGate(0);
			Set<QosGate> qosGates = new HashSet<QosGate>(Arrays.asList(memberV1.getOutputGate(0),
					memberV1.getOutputGate(1)));
			int nonNullCount = 0;
			int numberOfTotalEdges = 0;
			for (QosGate g : qosGates)
				if (g != null) {
					nonNullCount++;
					numberOfTotalEdges += g.getNumberOfEdges();
				}
			assertTrue(
					"There should be at least output gate for each physical vertex",
					nonNullCount > 0);
			assertEquals(
					"Bipartite dist should amount to 5 edges per member in total",
					5, numberOfTotalEdges);
		}
		// check graph sinks
		Set<QosGroupVertex> sinks = graph1.getEndVertices();
		assertEquals("there should be 1 sink-group-vertex", 1, sinks.size());
		QosGroupVertex sinkGroupVertex = sinks.iterator().next();
		assertEquals("there should be 5 sink-members", 5,
				sinkGroupVertex.getNumberOfMembers());
		assertEquals("there should be no output gates", 0,
				sinkGroupVertex.getNumberOfOutputGates());
		assertEquals("there should be one input gate", 1,
				sinkGroupVertex.getNumberOfInputGates());
		// on member level there should be 5 edges connecting vertex4 to vertex5
		for (QosVertex v : sinkGroupVertex.getMembers()) {
			QosGate inputGate = v.getInputGate(0);
			assertTrue(inputGate != null);
			assertEquals("Each gate should have one edge", 1,
					inputGate.getNumberOfEdges());
			for (QosEdge e : inputGate.getEdges())
				assertEquals(
						"Each edge should be connected to an instance of vertex4",
						this.fix.vertex4, e.getOutputGate().getVertex()
								.getGroupVertex());
		}
		// now let's check "central" vertex4
		Iterator<QosGroupEdge> edgeIterator = sinkGroupVertex
				.getBackwardEdges().iterator();
		assertTrue("v5 should have backward edges", edgeIterator.hasNext());
		QosGroupEdge edgeBack = edgeIterator.next();
		assertNotNull("The should be a backward edge", edgeBack);
		QosGroupVertex v4 = edgeBack.getSourceVertex();
		assertEquals("vertex4", v4.getName());
		assertEquals("vertex4 should have one member instance", 1,
				v4.getNumberOfMembers());
		// backward edges should be either connected to vertex2 or vertex3
		for (QosGroupEdge ge : v4.getBackwardEdges())
			assertTrue(
					"backward edges of vertex 4 should be either connected to vertex2 or vertex3",
					supposedSplitVertices.contains(ge.getSourceVertex()));
		Iterator<QosVertex> v4MembersIterator = v4.getMembers().iterator();
		assertTrue("v4 should have members", v4MembersIterator.hasNext());
		QosVertex v4Member = v4MembersIterator.next();
		assertNotNull("The Member should not be null.", v4Member);
		QosGate inputGate0 = v4Member.getInputGate(0);
		int count0 = count(inputGate0.getEdges().iterator());
		assertEquals(2, count0); // going to vertex2
		QosGate inputGate1 = v4Member.getInputGate(1);
		int count1 = count(inputGate1.getEdges().iterator());
		assertEquals(3, count1); // going to vertex3
		for (QosEdge edge : inputGate0.getEdges())
			assertTrue(supposedSplitVertices.contains(edge.getOutputGate()
					.getVertex().getGroupVertex()));
	}

	@Test
	public void testMergeWithGates() throws Exception {
		// create sequence v1, v2, v4
		JobGraphSequence jobGraphSequence1 = new JobGraphSequence();
		jobGraphSequence1.addEdge(this.fix.jobVertex1.getID(), 0,
				this.fix.jobVertex2.getID(), 0);
		jobGraphSequence1.addVertex(this.fix.jobVertex2.getID(), this.fix.jobVertex2.getName(), 0, 0);
		jobGraphSequence1.addEdge(this.fix.jobVertex2.getID(), 0,
				this.fix.jobVertex4.getID(), 0);
		jobGraphSequence1.addVertex(this.fix.jobVertex4.getID(), this.fix.jobVertex4.getName(), 0, 0);
		JobGraphLatencyConstraint jobGraphLatencyConstraint1 = new JobGraphLatencyConstraint(
				jobGraphSequence1, 2000l);

		// sequence v1, v3, v4
		JobGraphSequence jobGraphSequence2 = new JobGraphSequence();
		jobGraphSequence2.addEdge(this.fix.jobVertex1.getID(), 1,
				this.fix.jobVertex3.getID(), 0);
		jobGraphSequence2.addVertex(this.fix.jobVertex3.getID(), this.fix.jobVertex3.getName(), 0, 0);
		jobGraphSequence2.addEdge(this.fix.jobVertex3.getID(), 0,
				this.fix.jobVertex4.getID(), 1);
		jobGraphSequence2.addVertex(this.fix.jobVertex4.getID(), this.fix.jobVertex4.getName(), 1, 0);
		JobGraphLatencyConstraint jobGraphLatencyConstraint2 = new JobGraphLatencyConstraint(
				jobGraphSequence2, 2000L);

		// create execution graph
		InstanceType instanceType = new InstanceType();
		InstanceManager instanceManager = mock(InstanceManager.class);
		when(instanceManager.getDefaultInstanceType()).thenReturn(instanceType);
		ExecutionGraph eg = new ExecutionGraph(this.fix.jobGraph,
				instanceManager);
		QosGraph qosGraph1 = QosGraphFactory.createConstrainedQosGraph(eg,
				jobGraphLatencyConstraint1);
		QosGraph qosGraph2 = QosGraphFactory.createConstrainedQosGraph(eg,
				jobGraphLatencyConstraint2);
		qosGraph1.merge(qosGraph2);

		this.checkGraphIntegrity(qosGraph1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testEvilMergeWithGates() throws Exception {
		// create sequence v1, v2, v4
		JobGraphSequence jobGraphSequence1 = new JobGraphSequence();
		jobGraphSequence1.addEdge(this.fix.jobVertex1.getID(), 0,
				this.fix.jobVertex2.getID(), 0);
		jobGraphSequence1.addVertex(this.fix.jobVertex2.getID(), this.fix.jobVertex2.getName(), 0, 0);
		jobGraphSequence1.addEdge(this.fix.jobVertex2.getID(), 0,
				this.fix.jobVertex4.getID(), 0);
		jobGraphSequence1.addVertex(this.fix.jobVertex4.getID(), this.fix.jobVertex4.getName(), 0, 0);
		JobGraphLatencyConstraint jobGraphLatencyConstraint1 = new JobGraphLatencyConstraint(
				jobGraphSequence1, 2000l);

		// sequence v1, v3, v4
		JobGraphSequence jobGraphSequence2 = new JobGraphSequence();
		jobGraphSequence2.addEdge(this.fix.jobVertex1.getID(), 0,
				this.fix.jobVertex3.getID(), 0);
		jobGraphSequence2.addVertex(this.fix.jobVertex3.getID(), this.fix.jobVertex3.getName(), 0, 0);
		jobGraphSequence2.addEdge(this.fix.jobVertex3.getID(), 0,
				this.fix.jobVertex4.getID(), 0);
		jobGraphSequence2.addVertex(this.fix.jobVertex4.getID(), this.fix.jobVertex4.getName(), 1, 0);
		JobGraphLatencyConstraint jobGraphLatencyConstraint2 = new JobGraphLatencyConstraint(
				jobGraphSequence2, 2000L);

		// create execution graph
		InstanceType instanceType = new InstanceType();
		InstanceManager instanceManager = mock(InstanceManager.class);
		when(instanceManager.getDefaultInstanceType()).thenReturn(instanceType);
		ExecutionGraph eg = new ExecutionGraph(this.fix.jobGraph,
				instanceManager);
		QosGraph qosGraph1 = QosGraphFactory.createConstrainedQosGraph(eg,
				jobGraphLatencyConstraint1);
		QosGraph qosGraph2 = QosGraphFactory.createConstrainedQosGraph(eg,
				jobGraphLatencyConstraint2);
		qosGraph1.merge(qosGraph2);

		this.checkGraphIntegrity(qosGraph1);
	}

	private void checkGraphIntegrity(QosGraph qosGraph1) {
		Set<String> expectedVertexNames = new HashSet<String>(Arrays.asList(
				this.fix.vertex2.getName(), this.fix.vertex3.getName(),
				this.fix.vertex4.getName()));
		for (QosGroupVertex gv : qosGraph1.getAllVertices()) {
			if (gv.getName().equals(this.fix.vertex2.getName())) { // vertex2
				assertEquals(1, gv.getNumberOfInputGates());
				assertEquals(1, gv.getNumberOfOutputGates());
				assertTrue(count(gv.getMembers().iterator()) > 0);
				for (QosVertex v : gv.getMembers()) {
					assertNotNull(v.getInputGate(0));
					assertNotNull(v.getOutputGate(0));
				}
			} else if (gv.getName().equals(this.fix.vertex3.getName())) { // vertex3
				assertEquals(1, gv.getNumberOfInputGates());
				assertEquals(1, gv.getNumberOfOutputGates());
				assertTrue(count(gv.getMembers().iterator()) > 0);
				for (QosVertex v : gv.getMembers()) {
					assertNotNull(v.getInputGate(0));
					assertNotNull(v.getOutputGate(0));
				}
			} else if (gv.getName().equals(this.fix.vertex4.getName())) { // vertex4
				assertEquals(2, gv.getNumberOfInputGates());
				// this time around we don't really care about the
				// output gates
				assertTrue(count(gv.getMembers().iterator()) > 0);
				for (QosVertex v : gv.getMembers()) {
					assertNotNull(v.getInputGate(0));
					assertNotNull(v.getInputGate(1));
				}
			}
			// mark this one as "iterated over"
			expectedVertexNames.remove(gv.getName());
		}
		// did we encounter all expected vertices?
		assertEquals(0, expectedVertexNames.size());
	}

	/**
	 * Util method counting the elements on an {@link Iterator}
	 * 
	 * @param it
	 *            any java {@link Iterator}
	 * @return the element count
	 */
	private static int count(Iterator<?> it) {
		int count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		return count;
	}

	/**
	 * Util method to convert a Collection of Strings to a String, like a
	 * implode or join function in other languages.
	 * 
	 * @param strings
	 *            a {@link Collection} of {@link String}s
	 * @return the reduced version
	 */
	private static String join(Collection<String> strings) {
		if (strings.isEmpty())
			return "";
		StringBuilder sb = new StringBuilder();
		for (String s : strings)
			sb.append(s).append(", ");
		sb.setLength(sb.length() - 2);
		return sb.toString();
	}

	private QosGraph createQosGraphWithConstraint(JobGraph jg,
			JobGraphLatencyConstraint constraint)
			throws GraphConversionException {
		InstanceType instanceType = new InstanceType();
		InstanceManager instanceManager = mock(InstanceManager.class);
		when(instanceManager.getDefaultInstanceType()).thenReturn(instanceType);
		ExecutionGraph eg = new ExecutionGraph(jg, instanceManager);
		return QosGraphFactory.createConstrainedQosGraph(eg, constraint);
	}

}
