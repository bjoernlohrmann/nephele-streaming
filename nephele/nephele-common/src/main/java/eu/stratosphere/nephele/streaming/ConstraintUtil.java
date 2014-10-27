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

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.AbstractJobInputVertex;
import eu.stratosphere.nephele.jobgraph.AbstractJobOutputVertex;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobEdge;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.util.SerializableArrayList;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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
	 *             If something goes wrong while serializing the constraints
	 *             (shouldn't happen).
	 * @throws IllegalArgumentException
	 *             If constraints could not be constructed to to invalid
	 *             input parameters.
	 */
	public static void defineLatencyConstraint(JobGraphSequence sequence,
			long maxLatencyInMillis, JobGraph jobGraph, String name) throws IOException {

		ensurePreconditions(sequence, jobGraph);

		JobGraphLatencyConstraint constraint = new JobGraphLatencyConstraint(
				sequence, maxLatencyInMillis, name);

		addConstraint(constraint, jobGraph);
	}

	public static void defineLatencyConstraint(JobGraphSequence sequence,
			long maxLatencyInMillis, JobGraph jobGraph) throws IOException {
		defineLatencyConstraint(sequence, maxLatencyInMillis, jobGraph, generateConstraintName(sequence, jobGraph));
	}

	public static void addConstraint(JobGraphLatencyConstraint constraint,
			JobGraph jobGraph) throws IOException {

		Configuration jobConfig = jobGraph.getJobConfiguration();
		SerializableArrayList<JobGraphLatencyConstraint> constraints = getConstraints(jobConfig);
		constraints.add(constraint);
		putConstraints(jobConfig, constraints);
	}

	private static void ensurePreconditions(JobGraphSequence sequence,
			JobGraph jobGraph) {

		if (sequence.getFirst().isVertex()) {
			for (Iterator<AbstractJobInputVertex> iter = jobGraph
					.getInputVertices(); iter.hasNext();) {
				if (sequence.getFirst().getVertexID()
						.equals(iter.next().getID())) {
					throw new IllegalArgumentException(
							"Cannot define latency constraint that includes an input vertex. ");
				}
			}
		}

		if (sequence.getLast().isVertex()) {
			for (Iterator<AbstractJobOutputVertex> iter = jobGraph
					.getOutputVertices(); iter.hasNext();) {
				if (sequence.getLast().getVertexID()
						.equals(iter.next().getID())) {
					throw new IllegalArgumentException(
							"Cannot define latency constraint that includes an output vertex. ");
				}
			}
		}
	}

	/**
	 * Defines a constraint between two vertices (begin and end vertex excluded).
	 */
	public static void defineAllLatencyConstraintsBetween(
			AbstractJobVertex begin, AbstractJobVertex end,
			long maxLatencyInMillis, String name) throws IOException {

		int constraintsDefined = 0;
		for (int i = 0; i < begin.getNumberOfForwardConnections(); i++) {
			for (int j = 0; j < end.getNumberOfBackwardConnections(); j++) {
				constraintsDefined += defineAllLatencyConstraintsBetweenHelper(
						begin, end, maxLatencyInMillis,
						-1, i, j, -1, name);
			}
		}
		if (constraintsDefined == 0) {
			throw new IllegalArgumentException(
					"Could not find any sequences between the given vertices. Are the vertices connected?");
		}
	}

	public static void defineAllLatencyConstraintsBetween(
			AbstractJobVertex begin, AbstractJobVertex end,
			long maxLatencyInMillis) throws IOException {

		defineAllLatencyConstraintsBetween(begin, end, maxLatencyInMillis, generateConstraintName(begin, end, false));
	}

	/**
	 * Defines a constraint between two vertices (begin and end vertex included).
	 */
	public static void defineAllLatencyConstraintsBetween(
			AbstractJobVertex begin, int beginInputGateIndex,
			AbstractJobVertex end, int endOutputGateIndex,
			long maxLatencyInMillis, String name) throws IOException {

		int constraintsDefined = 0;
		for (int i = 0; i < begin.getNumberOfForwardConnections(); i++) {
			for (int j = 0; j < end.getNumberOfBackwardConnections(); j++) {
				constraintsDefined += defineAllLatencyConstraintsBetweenHelper(
						begin, end, maxLatencyInMillis,
						beginInputGateIndex, i, j, endOutputGateIndex, name);
			}
		}
		if (constraintsDefined == 0) {
			throw new IllegalArgumentException(
					"Could not find any sequences between the given vertices. Are the vertices connected?");
		}
	}

	public static void defineAllLatencyConstraintsBetween(
			AbstractJobVertex begin, int beginInputGateIndex,
			AbstractJobVertex end, int endOutputGateIndex,
			long maxLatencyInMillis) throws IOException {

		defineAllLatencyConstraintsBetween(begin, beginInputGateIndex, end, endOutputGateIndex,
				maxLatencyInMillis, generateConstraintName(begin, end, true));
	}

	/**
	 * Finds all possible sequences starting and ending with the given edges. The given
	 * edges must be forward pointing {@link JobEdge} objects.
	 */
	public static void defineAllLatencyConstraintsBetween(JobEdge beginEdge,
			JobEdge endEdge, long maxLatencyInMillis, String name) throws IOException {

		ensureForwardEdge(beginEdge);
		ensureForwardEdge(endEdge);

		AbstractJobVertex excludedBeginVertex = beginEdge.getConnectedVertex()
				.getBackwardConnection(beginEdge.getIndexOfInputGate())
				.getConnectedVertex();

		AbstractJobVertex excludedEndVertex = endEdge.getConnectedVertex();

		int endInputGate = endEdge.getIndexOfInputGate();
		int beginOutputGate = beginEdge.getConnectedVertex().getBackwardConnection(beginEdge.getIndexOfInputGate())
				.getIndexOfInputGate();

		int constraintsDefined = defineAllLatencyConstraintsBetweenHelper(
				excludedBeginVertex, excludedEndVertex, maxLatencyInMillis,
				-1, beginOutputGate, endInputGate, -1, name);
		if (constraintsDefined == 0) {
			throw new IllegalArgumentException(
					"Could not find any sequences between the given vertices. Are the vertices connected?");
		}
	}

	public static void defineAllLatencyConstraintsBetween(JobEdge beginEdge,
			JobEdge endEdge, long maxLatencyInMillis) throws IOException {

		defineAllLatencyConstraintsBetween(beginEdge, endEdge, maxLatencyInMillis, generateConstraintName(beginEdge, endEdge));
	}

	private static void ensureForwardEdge(JobEdge edge) {

		boolean isForwardEdge = false;

		// try to treat edge as an forward pointing edge
		AbstractJobVertex edgeTarget = edge.getConnectedVertex();
		if (edgeTarget.getBackwardConnection(edge.getIndexOfInputGate()) != null) {
			JobEdge maybeBackwardEdge = edgeTarget.getBackwardConnection(edge
					.getIndexOfInputGate());
			if (maybeBackwardEdge != null) {
				AbstractJobVertex maybeEdgeSource = maybeBackwardEdge
						.getConnectedVertex();
				if (hasForwardEdge(maybeEdgeSource, edge)) {
					isForwardEdge = true;
				}
			}
		}

		if (!isForwardEdge) {
			throw new IllegalArgumentException(
					"Backward job graph edge specified. You must specify forward edges for constraint");
		}
	}

	private static boolean hasForwardEdge(AbstractJobVertex maybeEdgeSource,
			JobEdge edge) {

		for (int i = 0; i < maybeEdgeSource.getNumberOfForwardConnections(); i++) {
			if (maybeEdgeSource.getForwardConnection(i) == edge) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Finds all possible sequences between the given begin and end vertex, and invokes {@link
	 * #defineLatencyConstraint(JobGraphSequence, long, JobGraph)} on each. This is done using depth first traversal of
	 * the job graph, hence only invoke this method if the vertices of the graph have already been fully connected.
	 *
	 * @param begin
	 * 		The vertex to start the depth first traversal from. Note that if begin is an input vertex, you must set
	 * 		beginInputGate to -1.
	 * @param end
	 * 		The vertex where the depth first traversal ends. Note that if end is an output vertex, you must set endOutputGate
	 * 		to -1.
	 * @param maxLatencyInMillis
	 * 		See {@link #defineLatencyConstraint(JobGraphSequence, long, JobGraph)}.
	 * @param beginInputGate
	 * 		If beginInputGate != -1, then the begin vertex will be the first element of the found sequences. The
	 * 		beginInputGate specifies the input gate index to use for the first element (which is the begin vertex) of the
	 * 		latency constrained sequences. If beginInputGate == -1, then the edge originating from the output gate with index
	 * 		beginOutputGate of the begin vertex will be the first sequence elements.
	 * @param beginOutputGate
	 * 		The output gate index of the begin vertex from which the edge should start.
	 * @param endInputGate
	 * 		The inout gate index of the end vertex to which the last edge should connect.
	 * @param endOutputGate
	 * 		If endOutputGate != -1, then the end vertex will be the last element of the found sequences. The
	 * 		endVertexOutputGate specifies the  output gate index to use for the last element (which is the end vertex) of the
	 * 		latency constrained sequences. If endOutputGate == -1 then the edge arriving at the input gate with index
	 * 		endOutputGate of the end vertex will be the last sequence elements.
	 * @throws IOException
	 * 		If something goes wrong while serializing the constraints (shouldn't happen).
	 * @throws IllegalArgumentException
	 * 		If constraints could not be constructed to to invalid input parameters.
	 */
	public static void defineAllLatencyConstraintsBetween(
			AbstractJobVertex begin, AbstractJobVertex end, long maxLatencyInMillis,
			int beginInputGate, int beginOutputGate, int endInputGate, int endOutputGate,
			String name) throws IOException {

		int constraintsDefined = defineAllLatencyConstraintsBetweenHelper(
				begin, end, maxLatencyInMillis,
				beginInputGate, beginOutputGate, endInputGate, endOutputGate, name);

		if (constraintsDefined == 0) {
			throw new IllegalArgumentException(
					"Could not find any sequences between the given vertices. Are the vertices connected?");
		}
	}

	public static void defineAllLatencyConstraintsBetween(
			AbstractJobVertex begin, AbstractJobVertex end, long maxLatencyInMillis,
			int beginInputGate, int beginOutputGate, int endInputGate, int endOutputGate) throws IOException {

		defineAllLatencyConstraintsBetweenHelper(begin, end, maxLatencyInMillis,
				beginInputGate, beginOutputGate, endInputGate, endOutputGate, generateConstraintName(begin, end, true));
	}

	private static int defineAllLatencyConstraintsBetweenHelper(
			AbstractJobVertex begin, AbstractJobVertex end, long maxLatencyInMillis,
			int beginInputGate, int beginOutputGate, int endInputGate, int endOutputGate,
			String name) throws IOException {

		List<JobGraphSequence> sequences = findAllSequencesBetween(begin, end,
				beginInputGate, beginOutputGate, endInputGate, endOutputGate);

		for (JobGraphSequence sequence : sequences) {
			defineLatencyConstraint(sequence, maxLatencyInMillis,
					begin.getJobGraph(), name);
		}

		return sequences.size();
	}

	/**
	 * Traverses the job graph and finds all possible sequences between begin
	 * and end, where a sequence is a list of elements (e_1,...,e_n) where each
	 * element is either a vertex or an edge. If e_i is a vertex e_(i-1) and
	 * e_(i+1) will be edges.
	 * 
	 */
	private static List<JobGraphSequence> findAllSequencesBetween(AbstractJobVertex begin, AbstractJobVertex end,
			int beginInputGate, int beginOutputGate, int endInputGate, int endOutputGate) {
		JobGraphSequence stack = new JobGraphSequence();
		List<JobGraphSequence> toReturn = new LinkedList<JobGraphSequence>();

		if (begin.getNumberOfBackwardConnections() <= beginInputGate) {
			throw new IllegalArgumentException(String.format(
					"Invalid input gate index %d specified for vertex %s",
					beginInputGate, begin.getName()));
		}

		if (begin.getNumberOfForwardConnections() <= beginOutputGate) {
			throw new IllegalArgumentException(String.format(
					"Invalid input gate index %d specified for vertex %s",
					beginOutputGate, begin.getName()));
		}

		if (end.getNumberOfBackwardConnections() <= endInputGate) {
			throw new IllegalArgumentException(String.format(
					"Invalid output gate index %d specified for vertex %s",
					endInputGate, end.getName()));
		}

		if (end.getNumberOfForwardConnections() <= endOutputGate) {
			throw new IllegalArgumentException(String.format(
					"Invalid output gate index %d specified for vertex %s",
					endOutputGate, end.getName()));
		}


		if (beginInputGate != -1) {
			stack.addVertex(begin.getID(), begin.getName(), beginInputGate, beginOutputGate);
		}

		depthFirstSequenceEnumerate(begin.getForwardConnection(beginOutputGate), stack,
				toReturn, end, endInputGate);

		if (endOutputGate != -1) {
			for (JobGraphSequence sequence : toReturn) {
				sequence.addVertex(end.getID(), end.getName(), sequence.getLast()
						.getInputGateIndex(), endOutputGate);
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
			AbstractJobVertex end, int endInputGate) {

		AbstractJobVertex edgeTarget = forwardEdge.getConnectedVertex();
		JobEdge backwardEdge = edgeTarget.getBackwardConnection(forwardEdge
				.getIndexOfInputGate());

		stack.addEdge(backwardEdge.getConnectedVertex().getID(),
				backwardEdge.getIndexOfInputGate(), edgeTarget.getID(),
				forwardEdge.getIndexOfInputGate());

		if (edgeTarget == end) {
			// recursion ends here
			if (forwardEdge.getIndexOfInputGate() == endInputGate) {
				// only add stack to result if the last edge connects to the given input gate index of the end vertex
				resultAccumulator.add((JobGraphSequence) stack.clone());
			}
		} else {
			// recurse further
			for (int i = 0; i < edgeTarget.getNumberOfForwardConnections(); i++) {
				JobEdge nextEdge = edgeTarget.getForwardConnection(i);

				stack.addVertex(edgeTarget.getID(), edgeTarget.getName(),
						forwardEdge.getIndexOfInputGate(), i);
				depthFirstSequenceEnumerate(nextEdge, stack, resultAccumulator,
						end, endInputGate);
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

	/** Generate constraint name from given vertices names. */
	public static String generateConstraintName(String beginVertex, String endVertex, boolean vertexIncluded) {
		return beginVertex + " -> " + endVertex	+ " (start/end vertex " + (vertexIncluded ? "included" : "excluded") + ")";
	}

	public static String generateConstraintName(AbstractJobVertex begin, AbstractJobVertex end, boolean vertexIncluded) {
		return generateConstraintName(begin.getName(), end.getName(), vertexIncluded);
	}

	public static String generateConstraintName(JobEdge beginEdge, JobEdge endEdge) {
		if (beginEdge.getConnectedVertex().getBackwardConnection(beginEdge.getIndexOfInputGate()) == null)
			throw new IllegalArgumentException("Can't find predecessor vertex.");

		AbstractJobVertex excludedStartVertex =
				beginEdge.getConnectedVertex().getBackwardConnection(beginEdge.getIndexOfInputGate()).getConnectedVertex();

		return generateConstraintName(excludedStartVertex.getName(), endEdge.getConnectedVertex().getName(), false);
	}

	public static String generateConstraintName(JobGraphSequence sequence) {
		return generateConstraintName(sequence.getFirstVertex().getName(), sequence.getLastVertex().getName(), true);
	}

	public static String generateConstraintName(JobGraphSequence sequence, JobGraph jobGraph) {
		String beginVertex;
		String endVertex;

		if (sequence.getFirst().isVertex()) {
			beginVertex = sequence.getFirst().getName();
		} else {
			beginVertex = jobGraph.findVertexByID(sequence.getFirst().getSourceVertexID()).getName();
		}

		if (sequence.getLast().isVertex()) {
			endVertex = sequence.getLast().getName();
		} else {
			endVertex = jobGraph.findVertexByID(sequence.getLast().getTargetVertexID()).getName();
		}

		return generateConstraintName(beginVertex, endVertex, sequence.getFirst().isVertex());
	}
}
