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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.util.StreamPluginConfig;

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
			if(nextIsVertex) {
//				QosGroupVertexSummary vs = summary.getGroupVertexSummary(i);
//				builder.append(';');
//				builder.append(this.formatDouble(vs.getMeanVertexLatency()));
//				builder.append(';');
//				builder.append(this.formatDouble(vs.getMeanVertexLatencyVariance()));				
			} else {
				QosGroupEdgeSummary ve = summary.getGroupEdgeSummary(i);
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
				builder.append(this.formatDouble(ve.getMeanConsumerVertexInterarrivalTimeVariance()));
				builder.append(';');
				builder.append(this.formatDouble(ve.getMeanConsumerVertexLatency()));
				builder.append(';');
				builder.append(this.formatDouble(ve.getMeanConsumerVertexLatencyVariance()));
			}			
			nextIsVertex = !nextIsVertex;
		}
	}

	private String formatDouble(double doubleValue) {
		return String.format("%.2f", doubleValue);
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
			if (sequenceElement.isVertex()) {
//				builder.append(';');
//				builder.append(sequenceElement.getName()+"Mean");
//				builder.append(';');
//				builder.append(sequenceElement.getName()+"Var");
			} else {
				builder.append(';');
				builder.append("edge" + edgeIndex + "obl");
				builder.append(';');
				builder.append("edge" + edgeIndex);
				builder.append(';');
				builder.append("edge" + edgeIndex + "Emit");
				builder.append(';');
				builder.append("edge" + edgeIndex + "Consume");
				builder.append(';');
				builder.append("edge" + edgeIndex +"IAMean");
				builder.append(';');
				builder.append("edge" + edgeIndex +"IAVar");
				builder.append(';');
				builder.append("edge" + edgeIndex +"IRMean");
				builder.append(';');
				builder.append("edge" + edgeIndex +"IRVar");
				
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
