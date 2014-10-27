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

import java.io.IOException;

import org.codehaus.jettison.json.JSONException;

import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.QosConstraintSummary;

public abstract class AbstractQosLogger {
	protected long loggingInterval;

	public AbstractQosLogger(long loggingInterval) {
		this.loggingInterval = loggingInterval;
	}

	public abstract void logSummary(QosConstraintSummary summary) throws IOException, JSONException;

	protected long getLogTimestamp() {
		return QosUtils.alignToInterval(System.currentTimeMillis(), this.loggingInterval);
	}

	public void close() throws IOException {
	};
}
