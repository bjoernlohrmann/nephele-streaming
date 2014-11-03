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

import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class stores information about the latency as well as record
 * consumption and emission rate of a vertex (task).
 * 
 * @author warneke, Bjoern Lohrmann
 */
public final class VertexStatistics extends AbstractQosReportRecord {

	private QosReporterID.Vertex reporterID;
	private int counter;
	private double vertexLatency;
	private double vertexLatencyVariance;
	private double recordsConsumedPerSec;
	private double recordsEmittedPerSec;
	private double interArrivalTime;
	private double interArrivalTimeVariance;

	public VertexStatistics(QosReporterID.Vertex reporterID, double vertexLatency, double vertexLatencyVariance,
			double recordsConsumedPerSec, double recordsEmittedPerSec, double interArrivalTime,
			double interArrivalTimeVariance) {
		this.reporterID = reporterID;
		this.vertexLatency = vertexLatency;
		this.vertexLatencyVariance = vertexLatencyVariance;
		this.recordsConsumedPerSec = recordsConsumedPerSec;
		this.recordsEmittedPerSec = recordsEmittedPerSec;
		this.interArrivalTime = interArrivalTime;
		this.interArrivalTimeVariance = interArrivalTimeVariance;
		this.counter = 1;
	}

	@Deprecated
	public VertexStatistics(final QosReporterID.Vertex reporterID,
			final double vertexLatency, double recordsConsumedPerSec, double recordEmittedPerSec) {
		this(reporterID, vertexLatency, -1, recordsConsumedPerSec, recordEmittedPerSec, -1, -1);
	}

	/**
	 * Default constructor for the deserialization of the object.
	 */
	public VertexStatistics() {
	}

	public QosReporterID.Vertex getReporterID() {
		return this.reporterID;
	}

	/**
	 * Returns the task latency in milliseconds.
	 *
	 * @return the task latency in milliseconds
	 */
	public double getVertexLatency() {
		if (vertexLatency == -1) {
			return -1;
		}

		return this.vertexLatency / this.counter;
	}

	public double getVertexLatencyVariance() {
		return vertexLatencyVariance;
	}

	public double getRecordsConsumedPerSec() {
		if (recordsConsumedPerSec == -1) {
			return -1;
		}

		return recordsConsumedPerSec / this.counter;
	}

	public double getRecordsEmittedPerSec() {
		if (recordsEmittedPerSec == -1) {
			return -1;
		}

		return recordsEmittedPerSec / this.counter;
	}

	public double getInterArrivalTime() {
		if (interArrivalTime == -1) {
			return -1;
		}

		return interArrivalTime / counter;
	}

	public double getInterArrivalTimeVariance() {
		if (interArrivalTimeVariance == -1) {
			return -1;
		}

		return interArrivalTimeVariance / counter;
	}

	public void add(VertexStatistics vertexStats) {
		this.counter++;

		if (vertexLatency != -1) {
			this.vertexLatency += vertexStats.getVertexLatency();
		}

		if (vertexLatencyVariance != -1) {
			this.vertexLatencyVariance = vertexStats.getVertexLatencyVariance();
		}

		if (recordsConsumedPerSec != -1) {
			this.recordsConsumedPerSec += vertexStats
					.getRecordsConsumedPerSec();
		}

		if (recordsEmittedPerSec != -1) {
			this.recordsEmittedPerSec += vertexStats.getRecordsEmittedPerSec();
		}

		if (interArrivalTime != -1) {
			this.interArrivalTime = vertexStats.getInterArrivalTime();
		}

		if (interArrivalTimeVariance != -1) {
			this.interArrivalTimeVariance = vertexStats.getInterArrivalTimeVariance();
		}
	}

	@Override
	public boolean equals(Object otherObj) {
		boolean isEqual = false;
		if (otherObj instanceof VertexStatistics) {
			VertexStatistics other = (VertexStatistics) otherObj;
			isEqual = other.getReporterID().equals(this.getReporterID())
					&& other.getVertexLatency() == this.getVertexLatency();
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
		return new HashCodeBuilder().append(this.vertexLatency)
				.append(this.reporterID).toHashCode();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		this.reporterID.write(out);
		out.writeDouble(this.getVertexLatency());
		out.writeDouble(this.getVertexLatencyVariance());
		out.writeDouble(this.getRecordsConsumedPerSec());
		out.writeDouble(this.getRecordsEmittedPerSec());
		out.writeDouble(this.getInterArrivalTime());
		out.writeDouble(this.getInterArrivalTimeVariance());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		this.reporterID = new QosReporterID.Vertex();
		this.reporterID.read(in);
		this.vertexLatency = in.readDouble();
		this.vertexLatencyVariance = in.readDouble();
		this.recordsConsumedPerSec = in.readDouble();
		this.recordsEmittedPerSec = in.readDouble();
		this.interArrivalTime = in.readDouble();
		this.interArrivalTimeVariance = in.readDouble();
		this.counter = 1;
	}
}
