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

package eu.stratosphere.nephele.streaming.message.qosreport;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;

/**
 * This class stores information about the latency of a specific edge (channel).
 * 
 * @author warneke, Bjoern Lohrmann
 */
public final class EdgeLatency extends AbstractQosReportRecord {

	/**
	 * The {@link QosReporterID} of the reporter that sends the channel.
	 */
	private QosReporterID.Edge reporterID;

	private int counter;

	/**
	 * The channel latency in milliseconds
	 */
	private double edgeLatency;

	/**
	 * Constructs a new path latency object.
	 * 
	 * @param sourceChannelID
	 *            {@link ChannelID} representing the source end of the channel
	 * @param channelLatency
	 *            the channel latency in milliseconds
	 */
	public EdgeLatency(QosReporterID.Edge reporterID, double edgeLatency) {

		this.reporterID = reporterID;
		this.edgeLatency = edgeLatency;
		this.counter = 1;
	}

	/**
	 * Default constructor for the deserialization of the object.
	 */
	public EdgeLatency() {
	}

	public void add(EdgeLatency other) {
		this.counter++;
		this.edgeLatency += other.getEdgeLatency();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		this.reporterID.write(out);
		out.writeDouble(this.getEdgeLatency());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		this.reporterID = new QosReporterID.Edge();
		this.reporterID.read(in);
		this.edgeLatency = in.readDouble();
		this.counter = 1;
	}

	/**
	 * Returns the reporterID.
	 * 
	 * @return the reporterID
	 */
	public QosReporterID.Edge getReporterID() {
		return this.reporterID;
	}

	/**
	 * Returns the channel latency in milliseconds.
	 * 
	 * @return the channel latency in milliseconds
	 */
	public double getEdgeLatency() {

		return this.edgeLatency / this.counter;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		final StringBuilder str = new StringBuilder();
		str.append(this.reporterID.toString());
		str.append(": ");
		str.append(this.edgeLatency);

		return str.toString();
	}

	@Override
	public boolean equals(Object otherObj) {
		boolean isEqual = false;
		if (otherObj instanceof EdgeLatency) {
			EdgeLatency other = (EdgeLatency) otherObj;
			isEqual = other.reporterID.equals(this.reporterID)
					&& other.getEdgeLatency() == this.getEdgeLatency();
		}

		return isEqual;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return new HashCodeBuilder().append(this.edgeLatency)
				.append(this.reporterID).toHashCode();
	}
}
