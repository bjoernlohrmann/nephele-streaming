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

package eu.stratosphere.nephele.streaming.message.profiling;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;

/**
 * This class stores information about the latency of a specific channel.
 * 
 * @author warneke, Bjoern Lohrmann
 */
public final class ChannelLatency extends AbstractStreamProfilingRecord {

	/**
	 * The {@link QosReporterID} of the reporter that sends the channel.
	 */
	private QosReporterID.Edge reporterID;

	private int counter;

	/**
	 * The channel latency in milliseconds
	 */
	private double channelLatency;

	/**
	 * Constructs a new path latency object.
	 * 
	 * @param sourceChannelID
	 *            {@link ChannelID} representing the source end of the channel
	 * @param channelLatency
	 *            the channel latency in milliseconds
	 */
	public ChannelLatency(QosReporterID.Edge reporterID,
			double channelLatency) {

		this.reporterID = reporterID;
		this.channelLatency = channelLatency;
		this.counter = 1;
	}

	/**
	 * Default constructor for the deserialization of the object.
	 */
	public ChannelLatency() {
	}

	public void add(ChannelLatency other) {
		this.counter++;
		this.channelLatency += other.getChannelLatency();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		this.reporterID.write(out);
		out.writeDouble(this.getChannelLatency());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		this.reporterID = new QosReporterID.Edge();
		this.reporterID.read(in);
		this.channelLatency = in.readDouble();
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
	public double getChannelLatency() {

		return this.channelLatency / this.counter;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		final StringBuilder str = new StringBuilder();
		str.append(this.reporterID.toString());
		str.append(": ");
		str.append(this.channelLatency);

		return str.toString();
	}

	@Override
	public boolean equals(Object otherObj) {
		boolean isEqual = false;
		if (otherObj instanceof ChannelLatency) {
			ChannelLatency other = (ChannelLatency) otherObj;
			isEqual = other.reporterID.equals(this.reporterID)
					&& other.getChannelLatency() == this.getChannelLatency();
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
		final int prime = 31;
		long temp = Double.doubleToLongBits(this.channelLatency);
		int result = prime + (int) (temp ^ temp >>> 32);
		result = prime * result + this.reporterID.hashCode();
		return result;
	}
}
