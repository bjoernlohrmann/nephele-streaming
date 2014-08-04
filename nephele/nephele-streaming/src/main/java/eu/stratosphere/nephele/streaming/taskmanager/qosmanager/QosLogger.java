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

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.plugins.PluginManager;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.QosConstraintSummary;

/**
 * This class is used by Qos managers to log aggregated Qos report data for a
 * given Qos constraint.
 * 
 * @author Bjoern Lohrmann
 */
public class QosLogger {
	/**
	 * Provides access to the configuration entry which defines the log file
	 * location.
	 */
	private static final String LOGFILE_PATTERN_KEY = PluginManager
			.prefixWithPluginNamespace("streaming.qosmanager.logging.qos_statistics_filepattern");

	private static final String DEFAULT_LOGFILE_PATTERN = "/tmp/qos_statistics_%s";

	private BufferedWriter writer;

	private long loggingInterval;

	public QosLogger(JobGraphLatencyConstraint constraint, long loggingInterval) throws IOException {
		this.loggingInterval = loggingInterval;

		String logFile = GlobalConfiguration.getString(LOGFILE_PATTERN_KEY, DEFAULT_LOGFILE_PATTERN);
		if (logFile.contains("%s")) {
			logFile = String.format(logFile, constraint.getID().toString());
		}
		this.writer = new BufferedWriter(new FileWriter(logFile));
		this.writeHeaders(constraint.getSequence());
	}


	public void logSummary(QosConstraintSummary summary) throws IOException {

		StringBuilder builder = new StringBuilder();
		builder.append(this.getLogTimestamp());
		builder.append(';');
		builder.append(summary.getNoOfSequences());
		builder.append(';');

		this.appendSummaryLine(builder, summary);

		builder.append('\n');
		this.writer.write(builder.toString());
		this.writer.flush();
	}

	private void appendSummaryLine(StringBuilder builder, QosConstraintSummary summary) {
		builder.append(this.formatDouble(summary.getAvgSequenceLatency()));
		builder.append(';');
		builder.append(this.formatDouble(summary.getMinSequenceLatency()));
		builder.append(';');
		builder.append(this.formatDouble(summary.getMaxSequenceLatency()));
		
		double[][] memberStats = summary.getAggregatedMemberStatistics();

		for (int i = 0; i < memberStats.length; i++) {
			for (int j = 0; j < memberStats[i].length; j++) {
				builder.append(';');
				builder.append(this.formatDouble(memberStats[i][j]));
			}
		}
	}

	private String formatDouble(double doubleValue) {
		return String.format("%.2f", doubleValue);
	}

	private Object getLogTimestamp() {
		return QosUtils.alignToInterval(System.currentTimeMillis(),
				this.loggingInterval) / 1000;
	}

	private void writeHeaders(JobGraphSequence jobGraphSequence) throws IOException {

		StringBuilder builder = new StringBuilder();
		builder.append("timestamp;");
		builder.append("noOfActivePaths;");
		builder.append("avgTotalPathLatency;");
		builder.append("minPathLatency;");
		builder.append("maxPathLatency");

		int edgeIndex = 0;

		for (SequenceElement<JobVertexID> sequenceElement : jobGraphSequence) {
			if (sequenceElement.isVertex()) {
				builder.append(';');
				builder.append(sequenceElement.getName());
			} else {
				builder.append(';');
				builder.append("edge" + edgeIndex + "obl");
				builder.append(';');
				builder.append("edge" + edgeIndex);
				builder.append(';');
				builder.append("edge" + edgeIndex + "Emit");
				builder.append(';');
				builder.append("edge" + edgeIndex + "Consume");
				edgeIndex++;
			}
		}
		builder.append('\n');
		this.writer.write(builder.toString());
	}

}
