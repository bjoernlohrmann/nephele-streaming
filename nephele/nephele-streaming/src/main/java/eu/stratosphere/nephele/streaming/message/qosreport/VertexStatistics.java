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

import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.sampling.Sample;

/**
 * This class stores information about the latency as well as record consumption
 * and emission rate of a vertex (task).
 * 
 * @author warneke, Bjoern Lohrmann
 */
public final class VertexStatistics extends AbstractQosReportRecord {

	private QosReporterID.Vertex reporterID;
	private Sample vertexLatencyMillis;
	private double recordsConsumedPerSec;
	private double recordsEmittedPerSec;
	private Sample recordInterArrivalTimeMillis;

	public VertexStatistics(QosReporterID.Vertex reporterID,
			Sample vertexLatencyMillis, double recordsConsumedPerSec,
			double recordsEmittedPerSec, Sample recordInterArrivalTimeMillis) {

		this.reporterID = reporterID;
		this.vertexLatencyMillis = vertexLatencyMillis;
		this.recordsConsumedPerSec = recordsConsumedPerSec;
		this.recordsEmittedPerSec = recordsEmittedPerSec;
		this.recordInterArrivalTimeMillis = recordInterArrivalTimeMillis;
	}

	public VertexStatistics(QosReporterID.Vertex reporterID,
			double recordsEmittedPerSec) {
		this(reporterID, null, -1, recordsEmittedPerSec, null);
	}

	public VertexStatistics(QosReporterID.Vertex reporterID,
			double recordsConsumedPerSec, Sample recordInterArrivalTimeMillis) {
		this(reporterID, null, recordsConsumedPerSec, -1,
				recordInterArrivalTimeMillis);
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
	public Sample getVertexLatencyMillis() {
		return this.vertexLatencyMillis;
	}

	public double getRecordsConsumedPerSec() {
		return recordsConsumedPerSec;
	}

	public double getRecordsEmittedPerSec() {
		return recordsEmittedPerSec;
	}

	/**
	 * Returns the task latency in milliseconds.
	 * 
	 * @return the task latency in milliseconds
	 */
	public Sample getInterArrivalTimeMillis() {
		return recordInterArrivalTimeMillis;
	}

	public VertexStatistics fuseWith(VertexStatistics other) {

		boolean hasInputGate = reporterID.getInputGateID() != null;
		boolean hasOutputGate = reporterID.getOutputGateID() != null;
		
		
		VertexStatistics fused = new VertexStatistics(reporterID,
				vertexLatencyMillis, recordsConsumedPerSec,
				recordsEmittedPerSec, recordInterArrivalTimeMillis);

		if (hasInputGate) {
			fused.recordInterArrivalTimeMillis = recordInterArrivalTimeMillis
					.fuseWithDisjunctSample(other.getInterArrivalTimeMillis());

			fused.recordsConsumedPerSec = (recordsConsumedPerSec + other
					.getRecordsConsumedPerSec()) / 2;
		}

		if (hasOutputGate) {
			fused.recordsEmittedPerSec = (recordsEmittedPerSec + other
					.getRecordsEmittedPerSec()) / 2;
		}

		if (hasInputGate && hasOutputGate) {
			fused.vertexLatencyMillis = vertexLatencyMillis
					.fuseWithDisjunctSample(other.getVertexLatencyMillis());
		}
		
		return fused;
	}

	@Override
	public boolean equals(Object otherObj) {
		boolean isEqual = false;
		if (otherObj instanceof VertexStatistics) {
			VertexStatistics other = (VertexStatistics) otherObj;
			isEqual = other.getReporterID().equals(this.getReporterID())
					&& other.getVertexLatencyMillis() == this
							.getVertexLatencyMillis();
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
		return new HashCodeBuilder().append(this.vertexLatencyMillis.getMean())
				.append(this.reporterID).toHashCode();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		this.reporterID.write(out);

		boolean hasInputGate = reporterID.getInputGateID() != null;
		boolean hasOutputGate = reporterID.getOutputGateID() != null;

		if (hasInputGate) {
			recordInterArrivalTimeMillis.write(out);
			out.writeDouble(this.getRecordsConsumedPerSec());
		}

		if (hasOutputGate) {
			out.writeDouble(this.getRecordsEmittedPerSec());
		}

		if (hasInputGate && hasOutputGate) {
			vertexLatencyMillis.write(out);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		this.reporterID = new QosReporterID.Vertex();
		this.reporterID.read(in);

		boolean hasInputGate = reporterID.getInputGateID() != null;
		boolean hasOutputGate = reporterID.getOutputGateID() != null;

		if (hasInputGate) {
			recordInterArrivalTimeMillis = new Sample();
			recordInterArrivalTimeMillis.read(in);
			this.recordsConsumedPerSec = in.readDouble();
		}

		if (hasOutputGate) {
			this.recordsEmittedPerSec = in.readDouble();
		}

		if (hasInputGate && hasOutputGate) {
			vertexLatencyMillis = new Sample();
			vertexLatencyMillis.read(in);
		}
	}
}
