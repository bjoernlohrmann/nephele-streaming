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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.AbstractJobInputVertex;
import eu.stratosphere.nephele.jobgraph.AbstractJobOutputVertex;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobEdge;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.util.SerializableArrayList;

/**
 * This class contains utility methods to simplify the construction of
 * constraints.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class ConstraintUtil {

	private final static String STREAMING_LATENCY_CONSTRAINTS_KEY = "nephele.streaming.latency.constraints";

	/**
	 * Embeds a new latency constraint with the given sequence and maximum
	 * latency into the job configuration of the job graph.
	 * 
	 * @param sequence
	 * @param maxLatencyInMillis
	 * @param jobGraph
	 * @throws IOException
	 *             If the sequence begins with an input vertex or ends with an
	 *             output vertex, or if something goes wrong while serializing
	 *             the constraint.
	 */
	public static void defineLatencyConstraint(JobGraphSequence sequence,
			long maxLatencyInMillis, JobGraph jobGraph) throws IOException {

		ensurePreconditions(sequence, jobGraph);

		JobGraphLatencyConstraint constraint = new JobGraphLatencyConstraint(
				sequence, maxLatencyInMillis);

		addConstraint(constraint, jobGraph);
	}

	public static void addConstraint(JobGraphLatencyConstraint constraint,
			JobGraph jobGraph) throws IOException {

		Configuration jobConfig = jobGraph.getJobConfiguration();
		SerializableArrayList<JobGraphLatencyConstraint> constraints = getConstraints(jobConfig);
		constraints.add(constraint);
		putConstraints(jobConfig, constraints);
	}

	private static void ensurePreconditions(JobGraphSequence sequence,
			JobGraph jobGraph) throws IOException {

		if (sequence.getFirst().isVertex()) {
			for (Iterator<AbstractJobInputVertex> iter = jobGraph
					.getInputVertices(); iter.hasNext();) {
				if (sequence.getFirst().getVertexID()
						.equals(iter.next().getID())) {
					throw new IOException(
							"Cannot define latency constraint that includes an input vertex. ");
				}
			}
		}

		if (sequence.getLast().isVertex()) {
			for (Iterator<AbstractJobOutputVertex> iter = jobGraph
					.getOutputVertices(); iter.hasNext();) {
				if (sequence.getLast().getVertexID()
						.equals(iter.next().getID())) {
					throw new IOException(
							"Cannot define latency constraint that includes an output vertex. ");
				}
			}
		}
	}

	/**
	 * Convenience method equivalent to invoking {@see
	 * #defineAllLatencyConstraintsBetween(AbstractJobVertex, AbstractJobVertex,
	 * long, boolean, int, boolean, int) with all boolean parameters set to
	 * false.
	 */
	public static void defineAllLatencyConstraintsBetween(
			AbstractJobVertex begin, AbstractJobVertex end,
			long maxLatencyInMillis) throws IOException {

		defineAllLatencyConstraintsBetween(begin, end, maxLatencyInMillis,
				false, -1, false, -1);
	}

	/**
	 * Convenience method equivalent to invoking {@see
	 * #defineAllLatencyConstraintsBetween(AbstractJobVertex, AbstractJobVertex,
	 * long, boolean, int, boolean, int) with all boolean parameters set to
	 * true.
	 */
	public static void defineAllLatencyConstraintsBetween(
			AbstractJobVertex begin, int beginInputGateIndex,
			AbstractJobVertex end, int endOutputGateIndex,
			long maxLatencyInMillis) throws IOException {

		defineAllLatencyConstraintsBetween(begin, end, maxLatencyInMillis,
				true, beginInputGateIndex, true, endOutputGateIndex);
	}

	/**
	 * Convenience method equivalent to invoking {@see
	 * #defineAllLatencyConstraintsBetween(AbstractJobVertex, AbstractJobVertex,
	 * long, boolean, int, boolean, int) with excluded begin and end vertex.
	 */
	public static void defineAllLatencyConstraintsBetween(JobEdge beginEdge,
			JobEdge endEdge, long maxLatencyInMillis) throws IOException {

		AbstractJobVertex exludedBeginVertex = beginEdge.getConnectedVertex()
				.getBackwardConnection(beginEdge.getIndexOfInputGate())
				.getConnectedVertex();

		AbstractJobVertex exludedEndVertex = endEdge.getConnectedVertex();

		defineAllLatencyConstraintsBetween(exludedBeginVertex,
				exludedEndVertex, maxLatencyInMillis, false, -1, false, -1);
	}

	/**
	 * Convenience method that finds all possible sequences between the given
	 * begin and end vertex, and invokes {@see
	 * #defineLatencyConstraint(JobGraphSequence, long, JobGraph)} on each. This
	 * is done using depth first traversal of the job graph, hence only invoke
	 * this method if the vertices of the graph have already been fully
	 * connected.
	 * 
	 * @param begin
	 *            The vertex to start the depth first traversal from. Note that
	 *            if begin is an input vertex, you must set includeBeginVertex
	 *            to false.
	 * @param end
	 *            The vertex where the depth first traversal ends. Note that if
	 *            end is an output vertex, you must set includeEndVertex to
	 *            false.
	 * @param maxLatencyInMillis
	 *            See {@see #define(List, long, boolean, boolean)}.
	 * @param includeBeginVertex
	 *            If true, then the begin vertex will be the first element of
	 *            the found sequences. In this case you must specify a valid
	 *            beginVertexInputGate (which implies the begin vertex must not
	 *            be an input vertex because they have no input gates). If
	 *            false, then edge(s) originating from the begin vertex will be
	 *            the first sequence elements.
	 * @param beginVertexInputGate
	 *            If includeBeginVertex=true, the beginVertexInputGate specifies
	 *            the the input gate index to use for the first element (which
	 *            is the begin vertex) of the latency constrained sequences.
	 * @param includeEndVertex
	 *            If true, then the end vertex will be the last element of the
	 *            found sequences. In this case you must specify a valid
	 *            endVertexOutputGate (which implies the end vertex must not be
	 *            an output vertex because they have no output gates). If false
	 *            then edge(s) arriving at the end vertex will be the last
	 *            sequence elements.
	 * @param endVertexOutputGate
	 *            If includeEndVertex=true, the endVertexOutputGate specifies
	 *            the the output gate index to use for the last element (which
	 *            is the end vertex) of the latency constrained sequences.
	 * @throws IOException
	 *             If begin is an input vertex and includeBeginVertex=true, or
	 *             if end is an output vertex and includeEndVertex=true, or if
	 *             something goes wrong while serializing the constraints.
	 */
	public static void defineAllLatencyConstraintsBetween(
			AbstractJobVertex begin, AbstractJobVertex end,
			long maxLatencyInMillis, boolean includeBeginVertex,
			int beginVertexInputGate, boolean includeEndVertex,
			int endVertexOutputGate) throws IOException {

		List<JobGraphSequence> sequences = findAllSequencesBetween(begin, end,
				includeBeginVertex, beginVertexInputGate, includeEndVertex,
				endVertexOutputGate);

		if (sequences.isEmpty()) {
			throw new IOException(
					"Could not find any sequences between the given vertices. Are the vertices connected?");
		}

		for (JobGraphSequence sequence : sequences) {
			defineLatencyConstraint(sequence, maxLatencyInMillis,
					begin.getJobGraph());
		}
	}

	/**
	 * Traverses the job graph and finds all possible sequences between begin
	 * and end, where a sequence is a list of elements (e_1,...,e_n) where each
	 * element is either a vertex or an edge. If e_i is a vertex e_(i-1) and
	 * e_(i+1) will be edges.
	 * 
	 */
	private static List<JobGraphSequence> findAllSequencesBetween(
			AbstractJobVertex begin, AbstractJobVertex end,
			boolean includeBeginVertex, int beginVertexInputGate,
			boolean includeEndVertex, int endVertexOutputGate) {

		JobGraphSequence stack = new JobGraphSequence();
		List<JobGraphSequence> toReturn = new LinkedList<JobGraphSequence>();

		for (int i = 0; i < begin.getNumberOfForwardConnections(); i++) {
			if (includeBeginVertex) {
				stack.addVertex(begin.getID(), begin.getName(), beginVertexInputGate, i);
			}

			depthFirstSequenceEnumerate(begin.getForwardConnection(i), stack,
					toReturn, end);

			if (includeBeginVertex) {
				stack.removeLast();
			}
		}

		if (includeEndVertex) {
			for (JobGraphSequence sequence : toReturn) {
				sequence.addVertex(end.getID(), end.getName(), sequence.getLast()
						.getInputGateIndex(), endVertexOutputGate);
			}
		}
		return toReturn;
	}

	/**
	 * Adds the given forward-edge to the stack and recursively invokes itself
	 * for each forward-edge of the vertex connected to the given forward-edge,
	 * adding the connected vertex.
	 * 
	 * Terminates when the connected vertex is the given end vertex. The stack
	 * never includes the end vertex. Upon termination, the stack is cloned and
	 * added to the result accumulator.
	 */
	private static void depthFirstSequenceEnumerate(JobEdge forwardEdge,
			JobGraphSequence stack, List<JobGraphSequence> resultAccumulator,
			AbstractJobVertex end) {

		AbstractJobVertex edgeTarget = forwardEdge.getConnectedVertex();
		JobEdge backwardEdge = edgeTarget.getBackwardConnection(forwardEdge
				.getIndexOfInputGate());

		stack.addEdge(backwardEdge.getConnectedVertex().getID(),
				backwardEdge.getIndexOfInputGate(), edgeTarget.getID(),
				forwardEdge.getIndexOfInputGate());

		if (edgeTarget == end) {
			// recursion ends here
			resultAccumulator.add((JobGraphSequence) stack.clone());
		} else {
			// recurse further
			for (int i = 0; i < edgeTarget.getNumberOfForwardConnections(); i++) {
				JobEdge nextEdge = edgeTarget.getForwardConnection(i);

				stack.addVertex(edgeTarget.getID(), edgeTarget.getName(),
						forwardEdge.getIndexOfInputGate(), i);
				depthFirstSequenceEnumerate(nextEdge, stack, resultAccumulator,
						end);
				stack.removeLast();
			}
		}
		stack.removeLast();
	}

	private static void putConstraints(Configuration jobConfiguration,
			SerializableArrayList<JobGraphLatencyConstraint> constraints)
			throws IOException {

		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(byteOut);
		out.close();

		constraints.write(out);
		jobConfiguration.setBytes(STREAMING_LATENCY_CONSTRAINTS_KEY,
				byteOut.toByteArray());
	}

	public static SerializableArrayList<JobGraphLatencyConstraint> getConstraints(
			Configuration jobConfiguration) throws IOException {

		byte[] serializedConstraintList = jobConfiguration.getBytes(
				STREAMING_LATENCY_CONSTRAINTS_KEY, null);

		SerializableArrayList<JobGraphLatencyConstraint> toReturn = new SerializableArrayList<JobGraphLatencyConstraint>();
		if (serializedConstraintList != null) {
			DataInputStream in = new DataInputStream(new ByteArrayInputStream(
					serializedConstraintList));
			toReturn.read(in);
		}

		return toReturn;
	}

}
