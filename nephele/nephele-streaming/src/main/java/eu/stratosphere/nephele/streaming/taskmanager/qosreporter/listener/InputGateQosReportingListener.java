/***********************************************************************************************************************
 *
public dsdpublic * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.listener;

import eu.stratosphere.nephele.types.AbstractTaggableRecord;

/**
 * Callback interface used by the
 * {@link eu.stratosphere.nephele.streaming.taskmanager.runtime.io.StreamInputGate}
 * to signal that a new record has been received.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public interface InputGateQosReportingListener {

	public void recordReceived(int inputChannel, AbstractTaggableRecord record);

	public void tryingToReadRecord();

	public void inputBufferConsumed(int channelIndex,
			long bufferInterarrivalTimeNanos, int recordsReadFromBuffer);
}
