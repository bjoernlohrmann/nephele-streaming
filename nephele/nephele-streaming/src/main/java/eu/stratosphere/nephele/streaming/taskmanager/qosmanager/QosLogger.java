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
package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.util.StreamPluginConfig;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * This class is used by Qos managers to log aggregated Qos report data for a
 * given Qos constraint.
 * 
 * @author Bjoern Lohrmann
 */
public class QosLogger extends AbstractQosLogger {

	private BufferedWriter writer;


	public QosLogger(JobGraphLatencyConstraint constraint, long loggingInterval) throws IOException {
		super(loggingInterval);

		String logFile = StreamPluginConfig.getQosStatisticsLogfilePattern();
		if (logFile.contains("%s")) {
			logFile = String.format(logFile, constraint.getID().toString());
		}
		this.writer = new BufferedWriter(new FileWriter(logFile));
		this.writeHeaders(constraint.getSequence());
	}

	@Override
	public void logSummary(QosConstraintSummary summary) throws IOException {

		StringBuilder builder = new StringBuilder();
		builder.append(this.getLogTimestamp() / 1000);
		builder.append(';');
		builder.append(summary.getViolationReport().getNoOfSequences());
		builder.append(';');

		this.appendSummaryLine(builder, summary);

		builder.append('\n');
		this.writer.write(builder.toString());
		this.writer.flush();
	}

	private void appendSummaryLine(StringBuilder builder, QosConstraintSummary summary) {
		builder.append(this.formatDouble(summary.getViolationReport().getMeanSequenceLatency()));
		builder.append(';');
		builder.append(this.formatDouble(summary.getViolationReport().getMinSequenceLatency()));
		builder.append(';');
		builder.append(this.formatDouble(summary.getViolationReport().getMaxSequenceLatency()));
		
		boolean nextIsVertex = summary.doesSequenceStartWithVertex();
		
		for (int i = 0; i < summary.getSequenceLength(); i++) {
			if(!nextIsVertex) {
				QosGroupEdgeSummary ve = summary.getGroupEdgeSummary(i);
				builder.append(';');
				builder.append(ve.getActiveEmitterVertices());
				builder.append(';');
				builder.append(this.formatDouble(ve.getOutputBufferLatencyMean()));
				builder.append(';');
				builder.append(this.formatDouble(ve.getTransportLatencyMean()));
				builder.append(';');
				builder.append(this.formatDouble(ve.getMeanEmissionRate()));
				builder.append(';');
				builder.append(this.formatDouble(ve.getMeanConsumptionRate()));
				builder.append(';');
				builder.append(this.formatDouble(ve.getMeanConsumerVertexInterarrivalTime()));
				builder.append(';');
				builder.append(this.formatDouble(ve.getMeanConsumerVertexInterarrivalTimeCV()));
				builder.append(';');
				builder.append(ve.getActiveConsumerVertices());
				builder.append(';');
				builder.append(this.formatDouble(ve.getMeanConsumerVertexLatency()));
				builder.append(';');
				builder.append(this.formatDouble(ve.getMeanConsumerVertexLatencyCV()));
			}			
			nextIsVertex = !nextIsVertex;
		}
	}

	private String formatDouble(double doubleValue) {
		return String.format("%.6f", doubleValue);
	}

	private void writeHeaders(JobGraphSequence jobGraphSequence) throws IOException {

		StringBuilder builder = new StringBuilder();
		builder.append("timestamp;");
		builder.append("noOfActivePaths;");
		builder.append("avgTotalPathLatency;");
		builder.append("minPathLatency;");
		builder.append("maxPathLatency");

		int edgeIndex = 0;

		for (SequenceElement sequenceElement : jobGraphSequence) {
			if (sequenceElement.isEdge()) {
				builder.append(';');
				builder.append("edge" + edgeIndex + "EmitterCount");
				builder.append(';');
				builder.append("edge" + edgeIndex + "obl");
				builder.append(';');
				builder.append("edge" + edgeIndex);
				builder.append(';');
				builder.append("edge" + edgeIndex + "Emit");
				builder.append(';');
				builder.append("edge" + edgeIndex + "Consume");
				builder.append(';');
				builder.append("edge" + edgeIndex +"IA-Mean");
				builder.append(';');
				builder.append("edge" + edgeIndex +"IA-CV");
				builder.append(';');
				builder.append("edge" + edgeIndex + "ConsumerCount");
				builder.append(';');
				builder.append("edge" + edgeIndex +"IR-Mean");
				builder.append(';');
				builder.append("edge" + edgeIndex +"IR-CV");
				
				edgeIndex++;
			}
		}
		builder.append('\n');
		this.writer.write(builder.toString());
		this.writer.flush();
	}

	public void close() throws IOException {
		this.writer.close();
	}
}
