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
package eu.stratosphere.nephele.streaming.message.action;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraph;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosManagerID;

/**
 * Describes a Qos manager role.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class QosManagerConfig implements IOReadableWritable {
	
	private QosManagerID qosManagerID;

	private QosGraph shallowQosGraph;

	public QosManagerConfig() {
	}

	public QosManagerConfig(QosGraph shallowQosGraph, QosManagerID qosManagerID) {
		this.qosManagerID = qosManagerID;
		
		if (shallowQosGraph == null) {
			throw new RuntimeException("Need to provide a Qos graph!");
		}
		this.shallowQosGraph = shallowQosGraph;
	}

	/**
	 * Returns the Qos Managers shallow Qos Graph.
	 * 
	 * @return the shallowQosGraph
	 */
	public QosGraph getShallowQosGraph() {
		return this.shallowQosGraph;
	}

	public QosManagerID getQosManagerID() {
		return qosManagerID;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.io.IOReadableWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		this.qosManagerID.write(out);
		this.shallowQosGraph.write(out);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.nephele.io.IOReadableWritable#read(java.io.DataInput)
	 */
	@Override
	public void read(DataInput in) throws IOException {
		this.qosManagerID = new QosManagerID();
		this.qosManagerID.read(in);
		
		this.shallowQosGraph = new QosGraph();
		this.shallowQosGraph.read(in);
	}
}