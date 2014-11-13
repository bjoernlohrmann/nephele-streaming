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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.jobgraph.JobVertexID;

/**
 * This class is used to unambiguously specify constraints on the job graph
 * level. A sequence is essentially a linked list of connected job vertices and
 * edges and fully specifies the input/output gate indices. The first element of
 * a sequence can be a vertex or an edge, the same goes for the last element.
 * 
 * For convenience during sequence construction this class is a subclass of
 * LinkedList.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class JobGraphSequence extends LinkedList<SequenceElement>
		implements IOReadableWritable {

	private static final long serialVersionUID = 1199328037569471951L;

	private HashSet<JobVertexID> verticesInSequence;

	public JobGraphSequence() {
		this.verticesInSequence = new HashSet<JobVertexID>();
	}

	public void addVertex(JobVertexID vertexID, String vertexName, int inputGateIndex,
			int outputGateIndex) {

		this.add(new SequenceElement(
				vertexID, inputGateIndex, outputGateIndex, this.size(), vertexName));
		this.verticesInSequence.add(vertexID);
	}

	public void addEdge(JobVertexID sourceVertexID, int outputGateIndex,
			JobVertexID targetVertexID, int inputGateIndex) {
		
		this.add(new SequenceElement(sourceVertexID,
				outputGateIndex, targetVertexID, inputGateIndex, this.size(), "edge"));
	}

	public boolean isInSequence(JobVertexID vertexID) {
		return this.verticesInSequence.contains(vertexID);
	}

	public int getNumberOfEdges() {
		return this.size() - getNumberOfVertices();
	}

	public int getNumberOfVertices() {
		return this.verticesInSequence.size();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.size());
		for (SequenceElement element : this) {
			element.write(out);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException {
		int elementCount = in.readInt();
		for (int i = 0; i < elementCount; i++) {
			SequenceElement element = new SequenceElement();
			element.read(in);
			this.add(element);
			if (element.isVertex()) {
				this.verticesInSequence.add(element.getVertexID());
			}
		}
	}

	public Collection<JobVertexID> getVerticesInSequence() {
		return this.verticesInSequence;
	}

	/**
	 * Returns an order list of vertices including external start/end vertex if
	 * requested and this sequence start/ends with an edge.
	 */
	public Collection<JobVertexID> getVerticesForSequenceOrdered(boolean includeExternalStartEndVertex) {
		LinkedList<JobVertexID> vertices = new LinkedList<JobVertexID>();

		if (includeExternalStartEndVertex && getFirst().isEdge())
			vertices.add(getFirst().getSourceVertexID());

		for (SequenceElement element : this) {
			if (element.isVertex())
				vertices.add(element.getVertexID());
		}

		if (includeExternalStartEndVertex && getLast().isEdge())
			vertices.add(getLast().getTargetVertexID());

		return vertices;
	}

	public SequenceElement getFirstVertex() {
		for (Iterator<SequenceElement> it = iterator(); it.hasNext();) {
			SequenceElement current = it.next();
			if (current.isVertex())
				return current;
		}

		return null;
	}

	public SequenceElement getLastVertex() {
		for (Iterator<SequenceElement> it = descendingIterator(); it.hasNext();) {
			SequenceElement current = it.next();
			if (current.isVertex())
				return current;
		}

		return null;
	}
}
