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

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.mockito.Matchers;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionSignature;
import eu.stratosphere.nephele.executiongraph.ExecutionStage;
import eu.stratosphere.nephele.executiongraph.GraphConversionException;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.streaming.JobGraphLatencyConstraint;
import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.AbstractTask;

/**
 * This fixture represents a graph that looks like this:
 *
 * input ->(B) task_1 ->(P) -> task_2 ->(P) task_3 ->(B) Output
 *
 * where (B) stands for BIPARTITE {@link DistributionPattern}
 * and (P) stands for POINTWISE {@link DistributionPattern}
 *
 * There's two subtasks for every element in the {@link JobGraph}.
 *
 * As with the other fixture you'll have to use the following
 * powermock annotations to make this class work in a test setup.
 *
 * @PrepareForTest({ ExecutionSignature.class, AbstractInstance.class,
 *                   AllocatedResource.class })
 * @RunWith(PowerMockRunner.class)
 *
 */
public class QosGraphFixture2 {

    public JobGraph jobGraph;
    public JobInputVertex jobInputVertex;
    public JobTaskVertex jobTaskVertex1;
    public JobTaskVertex jobTaskVertex2;
    public JobTaskVertex jobTaskVertex3;
    public JobOutputVertex jobOutputVertex;
    public ExecutionGraph executionGraph;

    public ExecutionGroupVertex executionInputVertex;
    public ExecutionGroupVertex executionTaskVertex1;
    public ExecutionGroupVertex executionTaskVertex2;
    public ExecutionGroupVertex executionTaskVertex3;
    public ExecutionGroupVertex executionOutputVertex;
    public InstanceConnectionInfo[][] instanceConnectionInfos;

    /**
     * This constraint represents a sequence of the full graph
     *
     */
    public JobGraphLatencyConstraint constraintFull;

    /**
     * This constraint represents -> t2 ->
     *
     */
    public JobGraphLatencyConstraint constraintEdgeVertexEdge;

    /**
     * This constraint represents t1 -> t2 ->
     */
    public JobGraphLatencyConstraint constraintVertexEdgeVertexEdge;

    /**
     * i -> t1
     */
    public JobGraphLatencyConstraint constraintInputVertexEdgeVertex;

    /**
     * t1 -> t2 -> t3
     */
    public JobGraphLatencyConstraint constraintMiddleOfGraph;

    /**
     * Creates a new fixture instance.
     */
    public QosGraphFixture2() throws JobGraphDefinitionException, GraphConversionException {
        if (!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
            BasicConfigurator.configure();
            Logger.getRootLogger().setLevel(Level.WARN);
        }

        createJobGraph();
        createExecutionGraph();
        createConstaints();
    }

    /**
     * Builds the {@link JobGraph}
     *
     * @throws JobGraphDefinitionException
     */
    private void createJobGraph() throws JobGraphDefinitionException {
        jobGraph = new JobGraph();

        jobInputVertex = new JobInputVertex("vInput", jobGraph);
        jobInputVertex.setInputClass(QosGraphTestUtil.DummyInputTask.class);
        jobInputVertex.setNumberOfSubtasks(2);

        jobTaskVertex1 = new JobTaskVertex("vTask1", jobGraph);
        jobTaskVertex1.setTaskClass(QosGraphTestUtil.DummyTask.class);
        jobTaskVertex1.setNumberOfSubtasks(2);

        jobTaskVertex2 = new JobTaskVertex("vTask2", jobGraph);
        jobTaskVertex2.setTaskClass(QosGraphTestUtil.DummyTask.class);
        jobTaskVertex2.setNumberOfSubtasks(2);

        jobTaskVertex3 = new JobTaskVertex("vTask3", jobGraph);
        jobTaskVertex3.setTaskClass(QosGraphTestUtil.DummyTask.class);
        jobTaskVertex3.setNumberOfSubtasks(2);

        jobOutputVertex = new JobOutputVertex("vOutput", jobGraph);
        jobOutputVertex.setOutputClass(QosGraphTestUtil.DummyOutputTask.class);
        jobOutputVertex.setNumberOfSubtasks(2);

        jobInputVertex.connectTo(jobTaskVertex1, ChannelType.NETWORK, 
                DistributionPattern.BIPARTITE);
        jobTaskVertex1.connectTo(jobTaskVertex2, ChannelType.NETWORK,
                DistributionPattern.POINTWISE);
        jobTaskVertex2.connectTo(jobTaskVertex3, ChannelType.NETWORK,
                DistributionPattern.POINTWISE);
        jobTaskVertex3.connectTo(jobOutputVertex, ChannelType.NETWORK,
                DistributionPattern.BIPARTITE);
    }


    /**
     * Creates the executing graph from {@link #jobGraph} and
     * assigns faux {@link InstanceConnectionInfo} instances.
     *
     * @throws GraphConversionException
     */
    private void createExecutionGraph() throws GraphConversionException {
        mockStatic(ExecutionSignature.class);
        ExecutionSignature execSig = mock(ExecutionSignature.class);

        when(ExecutionSignature.createSignature(
                Matchers.eq(AbstractGenericInputTask.class),
                Matchers.any(JobID.class)
        )).thenReturn(execSig);

        when(ExecutionSignature.createSignature(
                Matchers.eq(AbstractTask.class),
                Matchers.any(JobID.class)
        )).thenReturn(execSig);

        when(ExecutionSignature.createSignature(
                Matchers.eq(AbstractOutputTask.class),
                Matchers.any(JobID.class)
        )).thenReturn(execSig);

        InstanceManager instanceManager = mock(InstanceManager.class);
        InstanceType instanceType = new InstanceType();
        when(instanceManager.getDefaultInstanceType()).thenReturn(instanceType);
        executionGraph = new ExecutionGraph(this.jobGraph, instanceManager);

        ExecutionStage executionStage = executionGraph.getStage(0);
        for (int i = 0; i < executionStage.getNumberOfStageMembers(); i++) {
            ExecutionGroupVertex v =
                    executionStage.getStageMember(i);
            if(v.getJobVertexID().equals(jobInputVertex.getID())) {
                this.executionInputVertex = v;
            }
            else if(v.getJobVertexID().equals(jobTaskVertex1.getID())) {
                this.executionTaskVertex1 = v;
            }
            else if(v.getJobVertexID().equals(jobTaskVertex2.getID())) {
                this.executionTaskVertex2 = v;
            }
            else if(v.getJobVertexID().equals(jobTaskVertex3.getID())) {
                this.executionTaskVertex3 = v;
            }
            else if(v.getJobVertexID().equals(jobOutputVertex.getID())) {
                this.executionOutputVertex = v;
            }
        }
        instanceConnectionInfos = new InstanceConnectionInfo[5][];
        instanceConnectionInfos[0] = QosGraphTestUtil
                .generateAndAssignInstances(executionInputVertex);
        instanceConnectionInfos[1] = QosGraphTestUtil
                .generateAndAssignInstances(executionTaskVertex1);
        instanceConnectionInfos[2] = QosGraphTestUtil
                .generateAndAssignInstances(executionTaskVertex2);
        instanceConnectionInfos[3] = QosGraphTestUtil
                .generateAndAssignInstances(executionTaskVertex3);
        instanceConnectionInfos[4] = QosGraphTestUtil
                .generateAndAssignInstances(executionOutputVertex);
    }


    /**
     *
     */
    private void createConstaints() {
        // Full
        JobGraphSequence fullSequence = new JobGraphSequence();
        fullSequence.addVertex(jobInputVertex.getID(), jobInputVertex.getName(), 0, 0);
        fullSequence.addEdge(jobInputVertex.getID(), 0, jobTaskVertex1.getID(), 0);
        fullSequence.addVertex(jobTaskVertex1.getID(), jobTaskVertex1.getName(), 0, 0);
        fullSequence.addEdge(jobTaskVertex1.getID(), 0, jobTaskVertex2.getID(), 0);
        fullSequence.addVertex(jobTaskVertex2.getID(), jobTaskVertex2.getName(), 0, 0);
        fullSequence.addEdge(jobTaskVertex2.getID(), 0, jobTaskVertex3.getID(), 0);
        fullSequence.addVertex(jobTaskVertex3.getID(), jobTaskVertex3.getName(), 0, 0);
        fullSequence.addEdge(jobTaskVertex3.getID(), 0, jobOutputVertex.getID(), 0);
        fullSequence.addVertex(jobOutputVertex.getID(), jobOutputVertex.getName(), 0, 0);
        constraintFull = new JobGraphLatencyConstraint(fullSequence, 2000l);

        // -> t2 ->
        JobGraphSequence edgeVertexEdge = new JobGraphSequence();
        edgeVertexEdge.addEdge(jobTaskVertex1.getID(), 0, jobTaskVertex2.getID(), 0);
        edgeVertexEdge.addVertex(jobTaskVertex2.getID(), jobTaskVertex2.getName(), 0, 0);
        edgeVertexEdge.addEdge(jobTaskVertex2.getID(), 0, jobTaskVertex3.getID(), 0);
        constraintEdgeVertexEdge = new JobGraphLatencyConstraint(edgeVertexEdge, 2000l);

        // t1 -> t2 ->
        JobGraphSequence vertexEdgeVertexEdge = new JobGraphSequence();
        vertexEdgeVertexEdge.addVertex(jobTaskVertex1.getID(), jobTaskVertex1.getName(), 0, 0);
        vertexEdgeVertexEdge.addEdge(jobTaskVertex1.getID(), 0, jobTaskVertex2.getID(), 0);
        vertexEdgeVertexEdge.addVertex(jobTaskVertex2.getID(), jobTaskVertex2.getName(), 0, 0);
        vertexEdgeVertexEdge.addEdge(jobTaskVertex2.getID(), 0, jobTaskVertex3.getID(), 0);
        constraintVertexEdgeVertexEdge = new JobGraphLatencyConstraint(vertexEdgeVertexEdge, 2000l);

        JobGraphSequence inputVertexEdgeVertex = new JobGraphSequence();
        inputVertexEdgeVertex.addVertex(jobInputVertex.getID(), jobInputVertex.getName(), 0, 0);
        inputVertexEdgeVertex.addEdge(jobInputVertex.getID(), 0, jobTaskVertex1.getID(), 0);
        inputVertexEdgeVertex.addVertex(jobTaskVertex1.getID(),jobTaskVertex1.getName(), 0, 0);
        constraintInputVertexEdgeVertex = new JobGraphLatencyConstraint(inputVertexEdgeVertex, 2000l);

        JobGraphSequence middle = new JobGraphSequence();
        middle.addVertex(jobTaskVertex1.getID(), jobTaskVertex1.getName(), 0, 0);
        middle.addEdge(jobTaskVertex1.getID(), 0, jobTaskVertex2.getID(), 0);
        middle.addVertex(jobTaskVertex2.getID(), jobTaskVertex2.getName(), 0, 0);
        middle.addEdge(jobTaskVertex2.getID(), 0, jobTaskVertex3.getID(), 0);
        middle.addVertex(jobTaskVertex3.getID(), jobTaskVertex3.getName(), 0, 0);
        constraintMiddleOfGraph = new JobGraphLatencyConstraint(middle, 2000l);
    }

}