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
package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

/**
 * Common interface for {@link QosVertex} and {@link QosEdge} objects. This
 * interface exists to provide a common supertype for these elements as we often
 * have to deal with sequences that mix vertices and edges.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public interface QosGraphMember {

	public boolean isVertex();

	public boolean isEdge();

}
