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

import eu.stratosphere.nephele.io.IOReadableWritable;

/**
 * This class can be used to define latency constraints on a job graph. A
 * job-graph constraint is a sequence of connected job vertices and edges within
 * the job graph for which the user has a required upper latency bound. The
 * first element of a sequence can be a job vertex or an edge. To specify a
 * latency constraint a user must be aware how much latency his application can
 * tolerate in order to still be useful. By defining a constraint the user
 * indicates to the Nephele framework that it should apply runtime optimizations
 * (e.g. adaptive output buffer sizing or dynamic task chaining) so that the
 * constraint is met.
 * 
 * Have a look at {@see ConstraintUtil} for some convenience methods on
 * constructing latency constraints.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class JobGraphLatencyConstraint implements IOReadableWritable {

	private LatencyConstraintID constraintID;

	private JobGraphSequence sequence;

	private long latencyConstraintInMillis;

	private String name;

	/**
	 * Public parameterless constructor for deserialization.
	 */
	public JobGraphLatencyConstraint() {
	}

	public JobGraphLatencyConstraint(JobGraphSequence sequence,
			long latencyConstraintInMillis, String name) {

		this.constraintID = new LatencyConstraintID();
		this.sequence = sequence;
		this.latencyConstraintInMillis = latencyConstraintInMillis;
		this.name = name;
	}

	public JobGraphLatencyConstraint(JobGraphSequence sequence,
			long latencyConstraintInMillis) {
		this(sequence, latencyConstraintInMillis, ConstraintUtil.generateConstraintName(sequence));
	}

	/**
	 * Returns the constraintID.
	 * 
	 * @return the constraintID
	 */
	public LatencyConstraintID getID() {
		return this.constraintID;
	}

	/**
	 * 
	 * @return the sequence of the connected vertices covered by the latency
	 *         constraint.
	 */
	public JobGraphSequence getSequence() {
		return this.sequence;
	}

	/**
	 * Returns the latencyConstraintInMillis.
	 * 
	 * @return the latencyConstraintInMillis
	 */
	public long getLatencyConstraintInMillis() {
		return this.latencyConstraintInMillis;
	}

	/**
	 * Returns the constraint name.
	 *
	 * @return the constraint name
	 */
	public String getName() {
		return name;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		this.constraintID.write(out);
		this.sequence.write(out);
		out.writeLong(this.latencyConstraintInMillis);
		out.writeUTF(this.name);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException {
		this.constraintID = new LatencyConstraintID();
		this.constraintID.read(in);
		this.sequence = new JobGraphSequence();
		this.sequence.read(in);
		this.latencyConstraintInMillis = in.readLong();
		this.name = in.readUTF();
	}
}
