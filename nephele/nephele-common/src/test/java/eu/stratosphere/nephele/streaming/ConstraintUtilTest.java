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
package eu.stratosphere.nephele.streaming;

import eu.stratosphere.nephele.jobgraph.JobEdge;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Bjoern Lohrmann
 *
 */
public class ConstraintUtilTest {
	
	private JobGraph jobGraph;
	
	private JobInputVertex inputVertex;
	
	private JobTaskVertex taskVertex1;
	
	private JobTaskVertex taskVertex2;
	
	private JobTaskVertex taskVertex3;
	
	private JobOutputVertex outputVertex;
	
	private JobOutputVertex outputVertex2;
	
	private JobOutputVertex outputVertex3;
	
	@Before
	public void setUp() throws JobGraphDefinitionException {
		this.jobGraph = new JobGraph("Test Job");
		this.inputVertex = new JobInputVertex("in1", this.jobGraph);
		this.taskVertex1 = new JobTaskVertex("task1", this.jobGraph);
		this.taskVertex2 = new JobTaskVertex("task2", this.jobGraph);
		this.taskVertex3 = new JobTaskVertex("task3", this.jobGraph);
		this.outputVertex = new JobOutputVertex("out1", this.jobGraph);
		this.outputVertex2 = new JobOutputVertex("out2", this.jobGraph);
		this.outputVertex3 = new JobOutputVertex("out3", this.jobGraph);
		
		this.inputVertex.connectTo(this.taskVertex1);
		this.inputVertex.connectTo(this.taskVertex3);
		this.taskVertex1.connectTo(this.outputVertex2);
		this.taskVertex1.connectTo(this.taskVertex2);
		this.taskVertex2.connectTo(this.outputVertex);
		this.taskVertex2.connectTo(this.outputVertex3);
		this.taskVertex3.connectTo(this.outputVertex);
	}

	@Test(expected=IllegalArgumentException.class)
	public void testIllegalBeginInputGateIndex() throws IOException {
		// taskVertex1 has no input gate 37
		ConstraintUtil.defineAllLatencyConstraintsBetween(this.taskVertex1, 37,
				this.taskVertex2, 0, 999);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testIllegalBeginInputGateIndex2() throws IOException {
		// taskVertex1 has no input gate 1
		ConstraintUtil.defineAllLatencyConstraintsBetween(this.taskVertex1, 1,
				this.taskVertex2, 0, 999);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testIllegalEndOutputGateIndex() throws IOException {
		// taskVertex2 has no output gate 37
		ConstraintUtil.defineAllLatencyConstraintsBetween(this.taskVertex1, 0,
				this.taskVertex2, 37, 999);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testIllegalEndOutputGateIndex2() throws IOException {
		// taskVertex2 has no output gate 2
		ConstraintUtil.defineAllLatencyConstraintsBetween(this.taskVertex1, 0,
				this.taskVertex2, 2, 999);
	}

	@Test
	public void testDefineAllBetweenVertices() throws IOException {
		assertEquals(0, ConstraintUtil.getConstraints(this.jobGraph.getJobConfiguration()).size());
		
		ConstraintUtil.defineAllLatencyConstraintsBetween(this.taskVertex1, 0,
				this.taskVertex2, 1, 999);
		
		List<JobGraphLatencyConstraint> constraints = ConstraintUtil
				.getConstraints(this.jobGraph.getJobConfiguration());
		
		assertEquals(1, constraints.size());
		JobGraphLatencyConstraint constraint = constraints.get(0);
		assertTrue(constraint.getID() != null);
		
		assertEquals(3, constraint.getSequence().size());
		SequenceElement first = constraint.getSequence().get(0);
		SequenceElement second = constraint.getSequence().get(1);
		SequenceElement third = constraint.getSequence().get(2);
		
		assertTrue(first.isVertex());
		assertFalse(second.isVertex());
		assertTrue(third.isVertex());
		
		assertEquals(this.taskVertex1.getID(), first.getVertexID());
		assertEquals(this.taskVertex1.getID(), second.getSourceVertexID());
		assertEquals(this.taskVertex2.getID(), second.getTargetVertexID());
		assertEquals(this.taskVertex2.getID(), third.getVertexID());
		
		assertEquals(0, first.getInputGateIndex());
		assertEquals(1, first.getOutputGateIndex());
		assertEquals(1, second.getOutputGateIndex());
		assertEquals(0, second.getInputGateIndex());
		assertEquals(0, third.getInputGateIndex());
		assertEquals(1, third.getOutputGateIndex());
		
		assertEquals(999, constraint.getLatencyConstraintInMillis());
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testIllegalEdgeSpecified() throws IOException {
		JobEdge edge1 = this.taskVertex1.getBackwardConnection(0);
		JobEdge edge2 = this.taskVertex1.getForwardConnection(1);
		ConstraintUtil.defineAllLatencyConstraintsBetween(edge1, edge2, 999);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testIllegalEdgeSpecified2() throws IOException {
		JobEdge edge1 = this.inputVertex.getForwardConnection(0);
		JobEdge edge2 = this.taskVertex2.getBackwardConnection(0);
		ConstraintUtil.defineAllLatencyConstraintsBetween(edge1, edge2, 999);
	}
	
	@Test
	public void testDefineAllBetweenEdges() throws IOException {
		assertEquals(0, ConstraintUtil.getConstraints(this.jobGraph.getJobConfiguration()).size());
		
		JobEdge edge1 = this.inputVertex.getForwardConnection(0);
		JobEdge edge2 = this.taskVertex1.getForwardConnection(1);
		ConstraintUtil.defineAllLatencyConstraintsBetween(edge1, edge2, 999);
		
		List<JobGraphLatencyConstraint> constraints = ConstraintUtil
				.getConstraints(this.jobGraph.getJobConfiguration());
		
		assertEquals(1, constraints.size());
		JobGraphLatencyConstraint constraint = constraints.get(0);
		assertTrue(constraint.getID() != null);
		
		assertEquals(3, constraint.getSequence().size());
		SequenceElement first = constraint.getSequence().get(0);
		SequenceElement second = constraint.getSequence().get(1);
		SequenceElement third = constraint.getSequence().get(2);
		
		assertFalse(first.isVertex());
		assertTrue(second.isVertex());
		assertFalse(third.isVertex());
		
		assertEquals(this.inputVertex.getID(), first.getSourceVertexID());
		assertEquals(this.taskVertex1.getID(), first.getTargetVertexID());
		assertEquals(this.taskVertex1.getID(), second.getVertexID());
		assertEquals(this.taskVertex1.getID(), third.getSourceVertexID());
		assertEquals(this.taskVertex2.getID(), third.getTargetVertexID());
		
		assertEquals(0, first.getOutputGateIndex());
		assertEquals(0, first.getInputGateIndex());
		assertEquals(0, second.getInputGateIndex());
		assertEquals(1, second.getOutputGateIndex());
		assertEquals(1, third.getOutputGateIndex());
		assertEquals(0, third.getInputGateIndex());
		
		assertEquals(999, constraint.getLatencyConstraintInMillis());
	}
	
	@Test
	public void testDefineOneConstraintBetweenEdges() throws IOException, JobGraphDefinitionException {

		// so both ends can be tested (beginOutputGate and endInputGate)
		taskVertex3.connectTo(taskVertex2);

		assertEquals(0, ConstraintUtil.getConstraints(this.jobGraph.getJobConfiguration()).size());
		
		JobEdge edge1 = this.inputVertex.getForwardConnection(1);
		JobEdge edge2 = this.taskVertex3.getForwardConnection(0);
		ConstraintUtil.defineAllLatencyConstraintsBetween(edge1, edge2, 999);
		
		List<JobGraphLatencyConstraint> constraints = ConstraintUtil
				.getConstraints(this.jobGraph.getJobConfiguration());

		assertEquals(1, constraints.size());
		JobGraphLatencyConstraint constraint = constraints.get(0);
		assertTrue(constraint.getID() != null);
		
		assertEquals(3, constraint.getSequence().size());
		SequenceElement first = constraint.getSequence().get(0);
		SequenceElement second = constraint.getSequence().get(1);
		SequenceElement third = constraint.getSequence().get(2);
		
		assertFalse(first.isVertex());
		assertTrue(second.isVertex());
		assertFalse(third.isVertex());
		
		assertEquals(this.inputVertex.getID(), first.getSourceVertexID());
		assertEquals(this.taskVertex3.getID(), first.getTargetVertexID());
		assertEquals(this.taskVertex3.getID(), second.getVertexID());
		assertEquals(this.taskVertex3.getID(), third.getSourceVertexID());
		assertEquals(this.outputVertex.getID(), third.getTargetVertexID());
		
		assertEquals(1, first.getOutputGateIndex());
		assertEquals(0, first.getInputGateIndex());
		assertEquals(0, second.getInputGateIndex());
		assertEquals(0, second.getOutputGateIndex());
		assertEquals(0, third.getOutputGateIndex());
		assertEquals(1, third.getInputGateIndex());
		
		assertEquals(999, constraint.getLatencyConstraintInMillis());
	}
	
	@Test
	public void testDefineAllBetweenVerticesBig() throws IOException {
		assertEquals(0,
				ConstraintUtil.getConstraints(this.jobGraph.getJobConfiguration()).size());
		
		ConstraintUtil.defineAllLatencyConstraintsBetween(this.inputVertex,
				this.outputVertex, 1000);
		
		List<JobGraphLatencyConstraint> constraints = ConstraintUtil
				.getConstraints(this.jobGraph.getJobConfiguration());
		
		assertEquals(2, constraints.size());
		
		JobGraphLatencyConstraint constraint = constraints.get(0);
		assertTrue(constraint.getID() != null);

		assertEquals(5, constraint.getSequence().size());
		SequenceElement first = constraint.getSequence().get(0);
		SequenceElement second = constraint.getSequence().get(1);
		SequenceElement third = constraint.getSequence().get(2);
		SequenceElement fourth= constraint.getSequence().get(3);
		SequenceElement fifth = constraint.getSequence().get(4);

		assertFalse(first.isVertex());
		assertTrue(second.isVertex());
		assertFalse(third.isVertex());
		assertTrue(fourth.isVertex());
		assertFalse(fifth.isVertex());
		
		assertEquals(this.inputVertex.getID(), first.getSourceVertexID());
		assertEquals(this.taskVertex1.getID(), first.getTargetVertexID());
		assertEquals(this.taskVertex1.getID(), second.getVertexID());
		assertEquals(this.taskVertex1.getID(), third.getSourceVertexID());
		assertEquals(this.taskVertex2.getID(), third.getTargetVertexID());
		assertEquals(this.taskVertex2.getID(), fourth.getVertexID());
		assertEquals(this.taskVertex2.getID(), fifth.getSourceVertexID());
		assertEquals(this.outputVertex.getID(), fifth.getTargetVertexID());

		assertEquals(0, first.getOutputGateIndex());
		assertEquals(0, first.getInputGateIndex());

		assertEquals(0, second.getInputGateIndex());
		assertEquals(1, second.getOutputGateIndex());
		
		assertEquals(1, third.getOutputGateIndex());
		assertEquals(0, third.getInputGateIndex());
		
		assertEquals(0, fourth.getInputGateIndex());
		assertEquals(0, fourth.getOutputGateIndex());

		assertEquals(0, fifth.getOutputGateIndex());
		assertEquals(0, fifth.getInputGateIndex());
		
		constraint = constraints.get(1);
		assertTrue(constraint.getID() != null);

		assertEquals(3, constraint.getSequence().size());
		first = constraint.getSequence().get(0);
		second = constraint.getSequence().get(1);
		third = constraint.getSequence().get(2);
		
		assertFalse(first.isVertex());
		assertTrue(second.isVertex());
		assertFalse(third.isVertex());

		assertEquals(this.inputVertex.getID(), first.getSourceVertexID());
		assertEquals(this.taskVertex3.getID(), first.getTargetVertexID());
		assertEquals(this.taskVertex3.getID(), second.getVertexID());
		assertEquals(this.taskVertex3.getID(), third.getSourceVertexID());
		assertEquals(this.outputVertex.getID(), third.getTargetVertexID());

		assertEquals(1, first.getOutputGateIndex());
		assertEquals(0, first.getInputGateIndex());

		assertEquals(0, second.getInputGateIndex());
		assertEquals(0, second.getOutputGateIndex());
		
		assertEquals(0, third.getOutputGateIndex());
		assertEquals(1, third.getInputGateIndex());
	}
	
	@Test
	public void testDefineAllOnMultigraph() throws JobGraphDefinitionException, IOException {
		// putting second channels here make the job graph a multigraph
		this.taskVertex1.connectTo(this.taskVertex2);
		this.taskVertex2.connectTo(this.outputVertex);
		
		ConstraintUtil.defineAllLatencyConstraintsBetween(this.taskVertex1, this.outputVertex, 999);
		
		List<JobGraphLatencyConstraint> constraints = ConstraintUtil
				.getConstraints(this.jobGraph.getJobConfiguration());
		
		assertEquals(4, constraints.size());
		
		for(JobGraphLatencyConstraint constraint : constraints) {
			assertTrue(constraint.getID() != null);
			assertEquals(3, constraint.getSequence().size());
			SequenceElement first = constraint.getSequence().get(0);
			SequenceElement second = constraint.getSequence().get(1);
			SequenceElement third = constraint.getSequence().get(2);
			assertFalse(first.isVertex());
			assertTrue(second.isVertex());
			assertFalse(third.isVertex());
			assertEquals(this.taskVertex2.getID(), second.getSourceVertexID());			
		}
		
		JobGraphLatencyConstraint constraint = constraints.get(0);
		SequenceElement first = constraint.getSequence().get(0);
		SequenceElement second = constraint.getSequence().get(1);
		SequenceElement third = constraint.getSequence().get(2);
		assertEquals(1, first.getOutputGateIndex());
		assertEquals(0, first.getInputGateIndex());
		assertEquals(0, second.getInputGateIndex());
		assertEquals(0, second.getOutputGateIndex());
		assertEquals(0, third.getOutputGateIndex());
		assertEquals(0, third.getInputGateIndex());
		
		constraint = constraints.get(1);
		first = constraint.getSequence().get(0);
		second = constraint.getSequence().get(1);
		third = constraint.getSequence().get(2);
		assertEquals(1, first.getOutputGateIndex());
		assertEquals(0, first.getInputGateIndex());
		assertEquals(0, second.getInputGateIndex());
		assertEquals(2, second.getOutputGateIndex());
		assertEquals(2, third.getOutputGateIndex());
		assertEquals(2, third.getInputGateIndex());
		
		constraint = constraints.get(2);
		first = constraint.getSequence().get(0);
		second = constraint.getSequence().get(1);
		third = constraint.getSequence().get(2);
		assertEquals(2, first.getOutputGateIndex());
		assertEquals(1, first.getInputGateIndex());
		assertEquals(1, second.getInputGateIndex());
		assertEquals(0, second.getOutputGateIndex());
		assertEquals(0, third.getOutputGateIndex());
		assertEquals(0, third.getInputGateIndex());
		
		constraint = constraints.get(3);
		first = constraint.getSequence().get(0);
		second = constraint.getSequence().get(1);
		third = constraint.getSequence().get(2);
		assertEquals(2, first.getOutputGateIndex());
		assertEquals(1, first.getInputGateIndex());
		assertEquals(1, second.getInputGateIndex());
		assertEquals(2, second.getOutputGateIndex());
		assertEquals(2, third.getOutputGateIndex());
		assertEquals(2, third.getInputGateIndex());		
	}
}
