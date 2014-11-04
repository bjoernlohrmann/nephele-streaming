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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.LatencyConstraintID;
import eu.stratosphere.nephele.streaming.SequenceElement;

/**
 * @author Bjoern Lohrmann
 */
public class QosGraph implements IOReadableWritable {

	private QosGraphID qosGraphID;

	private HashSet<QosGroupVertex> startVertices;

	private HashSet<QosGroupVertex> endVertices;

	private HashMap<LatencyConstraintID, JobGraphLatencyConstraint> constraints;

	private HashMap<JobVertexID, QosGroupVertex> vertexByID;

	public QosGraph() {
		this.qosGraphID = new QosGraphID();
		this.startVertices = new HashSet<QosGroupVertex>();
		this.endVertices = new HashSet<QosGroupVertex>();
		this.constraints = new HashMap<LatencyConstraintID, JobGraphLatencyConstraint>();
		this.vertexByID = new HashMap<JobVertexID, QosGroupVertex>();
	}

	public QosGraph(QosGroupVertex startVertex) {
		this();
		this.startVertices.add(startVertex);
		this.exploreForward(startVertex);
	}

	public QosGraph(QosGroupVertex startVertex,
			JobGraphLatencyConstraint constraint) {
		this(startVertex);
		this.addConstraint(constraint);
	}

	private void exploreForward(QosGroupVertex vertex) {
		this.vertexByID.put(vertex.getJobVertexID(), vertex);
		for (QosGroupEdge forwardEdge : vertex.getForwardEdges()) {
			this.exploreForward(forwardEdge.getTargetVertex());
		}

		if (vertex.getNumberOfOutputGates() == 0) {
			this.endVertices.add(vertex);
		}
	}

	public void mergeForwardReachableGroupVertices(QosGroupVertex templateVertex) {
		this.mergeForwardReachableGroupVertices(templateVertex, true);
	}

	/**
	 * Clones and adds all vertices/edges that can be reached via forward
	 * movement from the given vertex into this graph.
	 */
	public void mergeForwardReachableGroupVertices(
			QosGroupVertex templateVertex, boolean cloneMembers) {
		QosGroupVertex vertex = this.getOrCreate(templateVertex, cloneMembers);

		for (QosGroupEdge templateEdge : templateVertex.getForwardEdges()) {
			if (!vertex.hasOutputGate(templateEdge.getOutputGateIndex())) {
				QosGroupVertex edgeTarget = this.getOrCreate(
						templateEdge.getTargetVertex(), cloneMembers);
				vertex.wireTo(edgeTarget, templateEdge);
				this.wireMembersUsingTemplate(vertex, edgeTarget, templateEdge);

				// remove vertex from end vertices if necessary (vertex has an
				// output gate now)
				this.endVertices.remove(vertex);

				// remove edgeTarget from start vertices if necessary
				// (edgeTarget has an input gate now)
				this.startVertices.remove(edgeTarget);
			}
			// recursive call
			this.mergeForwardReachableGroupVertices(
					templateEdge.getTargetVertex(), cloneMembers);
		}

		if (vertex.getNumberOfInputGates() == 0) {
			this.startVertices.add(vertex);
		}

		if (vertex.getNumberOfOutputGates() == 0) {
			this.endVertices.add(vertex);
		}
	}

	private void wireMembersUsingTemplate(QosGroupVertex from,
			QosGroupVertex to, QosGroupEdge templateGroupEdge) {

		int outputGateIndex = templateGroupEdge.getOutputGateIndex();
		int inputGateIndex = templateGroupEdge.getInputGateIndex();

		for (int i = 0; i < from.getNumberOfMembers(); i++) {
			QosVertex fromMember = from.getMember(i);
			QosVertex templateMember = templateGroupEdge.getSourceVertex()
					.getMember(i);

			QosGate templateOutputGate = templateMember
					.getOutputGate(outputGateIndex);
			QosGate outputGate = fromMember.getOutputGate(outputGateIndex);
			if (outputGate == null) {
				outputGate = templateOutputGate.cloneWithoutEdgesAndVertex();
				fromMember.setOutputGate(outputGate);
			}

			this.addEdgesToOutputGate(outputGate, templateOutputGate, to,
					inputGateIndex);
		}
	}

	private void addEdgesToOutputGate(QosGate outputGate,
			QosGate templateOutputGate, QosGroupVertex to, int inputGateIndex) {

		for (QosEdge templateEdge : templateOutputGate.getEdges()) {
			QosEdge clonedEdge = templateEdge.cloneWithoutGates();
			clonedEdge.setOutputGate(outputGate);

			QosVertex toMember = to.getMember(templateEdge.getInputGate()
					.getVertex().getMemberIndex());

			QosGate inputGate = toMember.getInputGate(inputGateIndex);
			if (inputGate == null) {
				inputGate = templateEdge.getInputGate()
						.cloneWithoutEdgesAndVertex();
				toMember.setInputGate(inputGate);
			}

			clonedEdge.setInputGate(inputGate);
		}
	}

	private QosGroupVertex getOrCreate(QosGroupVertex cloneTemplate,
			boolean cloneMembers) {
		QosGroupVertex toReturn = this.vertexByID.get(cloneTemplate
				.getJobVertexID());

		if (toReturn == null) {
			toReturn = cloneMembers ? cloneTemplate.cloneWithoutEdges()
					: cloneTemplate.cloneWithoutMembersOrEdges();
			this.vertexByID.put(toReturn.getJobVertexID(), toReturn);
		}

		return toReturn;
	}

	public void mergeBackwardReachableGroupVertices(
			QosGroupVertex templateVertex) {
		this.mergeBackwardReachableGroupVertices(templateVertex, true);
	}

	/**
	 * Clones and adds all vertices/edges that can be reached via backwards
	 * movement from the given vertex into this graph.
	 */
	public void mergeBackwardReachableGroupVertices(
			QosGroupVertex templateVertex, boolean cloneMembers) {
		QosGroupVertex vertex = this.getOrCreate(templateVertex, cloneMembers);

		for (QosGroupEdge templateEdge : templateVertex.getBackwardEdges()) {
			if (!vertex.hasInputGate(templateEdge.getInputGateIndex())) {
				QosGroupVertex edgeSource = this.getOrCreate(
						templateEdge.getSourceVertex(), cloneMembers);
				edgeSource.wireTo(vertex, templateEdge);
				this.wireMembersUsingTemplate(edgeSource, vertex, templateEdge);

				// remove edgeSource from end vertices if necessary (edgeSource
				// has an
				// output gate now)
				this.endVertices.remove(edgeSource);

				// remove vertex from start vertices if necessary
				// (vertex has an input gate now)
				this.startVertices.remove(vertex);
			}
			// recursive call
			this.mergeBackwardReachableGroupVertices(templateEdge
					.getSourceVertex());
		}

		if (vertex.getNumberOfInputGates() == 0) {
			this.startVertices.add(vertex);
		}

		if (vertex.getNumberOfOutputGates() == 0) {
			this.endVertices.add(vertex);
		}
	}

	public void addConstraint(JobGraphLatencyConstraint constraint) {
		this.ensureGraphContainsConstrainedVertices(constraint);
		this.constraints.put(constraint.getID(), constraint);
	}

	private void ensureGraphContainsConstrainedVertices(
			JobGraphLatencyConstraint constraint) {
		for (SequenceElement seqElem : constraint.getSequence()) {
			if (seqElem.isEdge()) {
				if (!this.vertexByID.containsKey(seqElem.getSourceVertexID())
						|| !this.vertexByID.containsKey(seqElem
								.getTargetVertexID())) {
					throw new IllegalArgumentException(
							"Cannot add constraint to a graph that does not contain the constraint's vertices. This is a bug.");
				}
			}
		}
	}

	public Collection<JobGraphLatencyConstraint> getConstraints() {
		return this.constraints.values();
	}

	public JobGraphLatencyConstraint getConstraintByID(
			LatencyConstraintID constraintID) {
		return this.constraints.get(constraintID);
	}

	// public Iterable<QosGroupVertex> getConstrainedGroupVertices(
	// LatencyConstraintID constraintID) {
	//
	// final JobGraphLatencyConstraint constraint = this.constraints
	// .get(constraintID);
	//
	// return new SparseDelegateIterable<QosGroupVertex>(
	// new Iterator<QosGroupVertex>() {
	// Iterator<SequenceElement> sequenceIter = constraint
	// .getSequence().iterator();
	// QosGroupVertex current = null;
	// QosGroupVertex last = null;
	//
	// @Override
	// public boolean hasNext() {
	// while (this.sequenceIter.hasNext()
	// && this.current == null) {
	// SequenceElement seqElem = this.sequenceIter
	// .next();
	// if (!seqElem.isVertex()) {
	// QosGroupVertex sourceVertex = QosGraph.this.vertexByID
	// .get(seqElem.getSourceVertexID());
	//
	// if (this.last == sourceVertex) {
	// this.current = QosGraph.this.vertexByID
	// .get(seqElem.getTargetVertexID());
	// ;
	// } else {
	// this.current = sourceVertex;
	// }
	// }
	// }
	//
	// return this.current != null;
	// }
	//
	// @Override
	// public QosGroupVertex next() {
	// this.last = this.current;
	// this.current = null;
	// return this.last;
	// }
	//
	// @Override
	// public void remove() {
	// }
	// });
	// }

	/**
	 * Adds all vertices, edges and constraints of the given graph into this
	 * one.
	 */
	public void merge(QosGraph graph) {
		this.mergeVerticesAndEdges(graph);
		this.recomputeStartAndEndVertices();
		this.constraints.putAll(graph.constraints);
	}

	private void recomputeStartAndEndVertices() {
		this.startVertices.clear();
		this.endVertices.clear();

		for (QosGroupVertex groupVertex : this.vertexByID.values()) {
			if (groupVertex.getNumberOfInputGates() == 0) {
				this.startVertices.add(groupVertex);
			}

			if (groupVertex.getNumberOfOutputGates() == 0) {
				this.endVertices.add(groupVertex);
			}
		}
	}

	private void mergeVerticesAndEdges(QosGraph graph) {
		for (QosGroupVertex templateVertex : graph.vertexByID.values()) {
			QosGroupVertex edgeSource = this.getOrCreate(templateVertex, true);

			for (QosGroupEdge templateEdge : templateVertex.getForwardEdges()) {
				if (!edgeSource
						.hasOutputGate(templateEdge.getOutputGateIndex())) {
					QosGroupVertex egdeTarget = this.getOrCreate(
							templateEdge.getTargetVertex(), true);

					edgeSource.wireTo(egdeTarget, templateEdge);
					this.wireMembersUsingTemplate(edgeSource, egdeTarget,
							templateEdge);
				}
			}
		}
	}

	public Set<QosGroupVertex> getStartVertices() {
		return Collections.unmodifiableSet(this.startVertices);
	}

	public Set<QosGroupVertex> getEndVertices() {
		return Collections.unmodifiableSet(this.endVertices);
	}

	public QosGroupVertex getGroupVertexByID(JobVertexID vertexID) {
		return this.vertexByID.get(vertexID);
	}

	public Collection<QosGroupVertex> getAllVertices() {
		return Collections.unmodifiableCollection(this.vertexByID.values());
	}

	public int getNumberOfVertices() {
		return this.vertexByID.size();
	}

	/**
	 * Clones the group level elements of this QosGraph. It does however not
	 * clone the QosVertex and QosEdge members of the QosGroupVertex objects.
	 * The QosGroupVertex elements of the clone have no members.
	 * 
	 * @return A clone of this graph (without group vertex members).
	 */
	@SuppressWarnings("unchecked")
	public QosGraph cloneWithoutMembers() {
		QosGraph clone = new QosGraph();
		for (QosGroupVertex startVertex : this.startVertices) {
			clone.mergeForwardReachableGroupVertices(startVertex, false);
		}

		clone.constraints = (HashMap<LatencyConstraintID, JobGraphLatencyConstraint>) this.constraints
				.clone();
		return clone;
	}

	/**
	 * Returns the qosGraphID.
	 * 
	 * @return the qosGraphID
	 */
	public QosGraphID getQosGraphID() {
		return this.qosGraphID;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return this.qosGraphID.hashCode();
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
		QosGraph other = (QosGraph) obj;

		return this.qosGraphID.equals(other.qosGraphID);
	}

	/**
	 * Serializes the group structure of this QosGraph (without member vertices
	 * and member edges).
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		this.writeConstraints(out);
		this.writeGroupVertices(out);
		this.writeGroupEdges(out);
	}

	private void writeConstraints(DataOutput out) throws IOException {
		out.writeInt(this.constraints.size());
		for (JobGraphLatencyConstraint constraint : this.constraints.values()) {
			constraint.write(out);
		}
	}

	private void writeGroupEdges(DataOutput out) throws IOException {
		for (QosGroupVertex groupVertex : this.vertexByID.values()) {
			out.writeInt(groupVertex.getNumberOfOutputGates());
			if (groupVertex.getNumberOfOutputGates() > 0) {
				groupVertex.getJobVertexID().write(out);
				for (QosGroupEdge groupEdge : groupVertex.getForwardEdges()) {
					out.writeUTF(groupEdge.getDistributionPattern().name());

					DistributionPattern.values();
					out.writeInt(groupEdge.getInputGateIndex());
					out.writeInt(groupEdge.getOutputGateIndex());
					groupEdge.getTargetVertex().getJobVertexID().write(out);
				}
			}
		}
	}

	private void writeGroupVertices(DataOutput out) throws IOException {
		out.writeInt(this.vertexByID.size());
		for (QosGroupVertex groupVertex : this.vertexByID.values()) {
			groupVertex.getJobVertexID().write(out);
			out.writeUTF(groupVertex.getName());
		}
	}

	/**
	 * Deserializes the group structure of a QosGraph (without member vertices
	 * and member edges).
	 */
	@Override
	public void read(DataInput in) throws IOException {
		this.readConstraints(in);
		this.readGroupVertices(in);
		this.readGroupEdges(in);
		this.recomputeStartAndEndVertices();
	}

	private void readConstraints(DataInput in) throws IOException {
		int noOfConstraints = in.readInt();
		for (int i = 0; i < noOfConstraints; i++) {
			JobGraphLatencyConstraint constraint = new JobGraphLatencyConstraint();
			constraint.read(in);
			this.constraints.put(constraint.getID(), constraint);
		}
	}

	private void readGroupVertices(DataInput in) throws IOException {
		int noOfGroupVertices = in.readInt();
		for (int i = 0; i < noOfGroupVertices; i++) {
			JobVertexID jobVertexID = new JobVertexID();
			jobVertexID.read(in);
			String name = in.readUTF();
			QosGroupVertex groupVertex = new QosGroupVertex(jobVertexID, name);
			this.vertexByID.put(jobVertexID, groupVertex);
		}
	}

	private void readGroupEdges(DataInput in) throws IOException {
		int noOfVertices = this.vertexByID.size();
		for (int i = 0; i < noOfVertices; i++) {
			int noOfForwardEdges = in.readInt();
			if (noOfForwardEdges > 0) {
				JobVertexID sourceVertexID = new JobVertexID();
				sourceVertexID.read(in);
				QosGroupVertex sourceVertex = this.vertexByID
						.get(sourceVertexID);
				for (int j = 0; j < noOfForwardEdges; j++) {
					DistributionPattern distPattern = DistributionPattern
							.valueOf(in.readUTF());
					int inputGateIndex = in.readInt();
					int outputGateIndex = in.readInt();
					JobVertexID targetVertexID = new JobVertexID();
					targetVertexID.read(in);
					QosGroupVertex targetVertex = this.vertexByID
							.get(targetVertexID);
					new QosGroupEdge(distPattern, sourceVertex, targetVertex,
							outputGateIndex, inputGateIndex);
				}
			}
		}
	}

	/**
	 * Determines whether this Qos graph is shallow. A graph is shallow, if at
	 * least one of the group vertices does not have any members.
	 * 
	 * @return whether this Qos graph is shallow or not.
	 */
	public boolean isShallow() {
		boolean isShallow = false;

		for (QosGroupVertex groupVertex : this.getAllVertices()) {
			if (groupVertex.getNumberOfMembers() == 0) {
				isShallow = true;
				break;
			}
		}
		return isShallow;
	}
}
