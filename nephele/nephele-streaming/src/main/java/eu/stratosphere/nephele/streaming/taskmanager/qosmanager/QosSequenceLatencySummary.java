package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import java.util.List;

import eu.stratosphere.nephele.streaming.JobGraphSequence;
import eu.stratosphere.nephele.streaming.SequenceElement;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers.ValueHistory;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.EdgeQosData;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosEdge;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosGraphMember;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosVertex;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.VertexQosData;

import java.util.List;

public class QosSequenceLatencySummary {
	
	private final int[][] inputOutputGateCombinations;
	private final double memberLatencies[][];
	private int noOfEdges;
	private int noOfVertices;
	private double sequenceLatency;
	private double outputBufferLatencySum;
	private boolean isMemberQosDataFresh;

	public QosSequenceLatencySummary(JobGraphSequence jobGraphSequence) {
		this.inputOutputGateCombinations = new int[jobGraphSequence.size()][];
		this.noOfEdges = 0;
		this.noOfVertices = 0;
		
		this.memberLatencies = new double[jobGraphSequence.size()][];
		for (SequenceElement sequenceElement : jobGraphSequence) {
			int index = sequenceElement.getIndexInSequence();

			this.inputOutputGateCombinations[index] = new int[2];
			if (sequenceElement.isVertex()) {
				this.noOfVertices++;
				this.memberLatencies[index] = new double[4];
				this.inputOutputGateCombinations[index][0] = sequenceElement
						.getInputGateIndex();
				this.inputOutputGateCombinations[index][1] = sequenceElement
						.getOutputGateIndex();
			} else {
				this.noOfEdges++;
				this.memberLatencies[index] = new double[2];
				this.inputOutputGateCombinations[index][0] = sequenceElement
						.getOutputGateIndex();
				this.inputOutputGateCombinations[index][1] = sequenceElement
						.getInputGateIndex();
			}
		}
	}
	
	public void update(List<QosGraphMember> sequenceMembers) {
		this.sequenceLatency = 0;
		this.outputBufferLatencySum = 0;
		this.isMemberQosDataFresh = true;
		
		int index = 0;
		for (QosGraphMember member : sequenceMembers) {
			if (member.isVertex()) {
				VertexQosData vertexQos = ((QosVertex) member).getQosData();

				int inputGateIndex = this.inputOutputGateCombinations[index][0];
				int outputGateIndex = this.inputOutputGateCombinations[index][1];

				this.memberLatencies[index][0] = vertexQos.getLatencyInMillis(inputGateIndex, outputGateIndex);
				this.memberLatencies[index][1] = vertexQos.getLatencyVarianceInMillis(inputGateIndex, outputGateIndex);
				this.memberLatencies[index][2] = vertexQos.getInterArrivalTimeInMillis(inputGateIndex);
				this.memberLatencies[index][3] = vertexQos.getInterArrivalTimeVarianceInMillis(inputGateIndex);
				sequenceLatency += this.memberLatencies[index][0];
			} else {
				EdgeQosData edgeQos = ((QosEdge) member).getQosData();
				double channelLatency = edgeQos.getChannelLatencyInMillis();
				double outputBufferLatency = Math.min(channelLatency, edgeQos.getOutputBufferLifetimeInMillis() / 2);
				
				this.memberLatencies[index][0] = outputBufferLatency;
				this.memberLatencies[index][1] = channelLatency - outputBufferLatency;
				sequenceLatency += channelLatency;
				outputBufferLatencySum += outputBufferLatency;
				this.isMemberQosDataFresh = this.isMemberQosDataFresh && hasFreshValues((QosEdge) member);
			}

			index++;
		}
	}
	
	private boolean hasFreshValues(QosEdge edge) {
		EdgeQosData edgeQos = edge.getQosData();

		ValueHistory<Integer> targetOblHistory = edgeQos.getTargetOblHistory();
		return !targetOblHistory.hasEntries()
				|| edgeQos.hasNewerData(targetOblHistory.getLastEntry().getTimestamp());
	}

	public double[][] getMemberLatencies() {
		return memberLatencies;
	}

	public double getSequenceLatency() {
		return sequenceLatency;
	}

	public double getOutputBufferLatencySum() {
		return outputBufferLatencySum;
	}
	
	public boolean isMemberQosDataFresh() {
		return this.isMemberQosDataFresh;
	}

	public int getNoOfEdges() {
		return noOfEdges;
	}

	public int getNoOfVertices() {
		return noOfVertices;
	}
	
	public double getNonOutputBufferLatency() {
		return this.sequenceLatency - this.outputBufferLatencySum;
	}
}
