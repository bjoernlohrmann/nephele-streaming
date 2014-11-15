package eu.stratosphere.nephele.streaming.jobmanager.autoscaling.optimization;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosGroupEdgeSummary;

public class GG1ServerTest {

	private QosGroupEdgeSummary edge1;
	private QosGroupEdgeSummary edge2;

	public static QosGroupEdgeSummary getEdge1() {
		QosGroupEdgeSummary edge1 = new QosGroupEdgeSummary();
		edge1.setMeanEmissionRate(30000.31);
		edge1.setActiveEmitterVertices(50);
		edge1.setActiveConsumerVertices(200);
		edge1.setMeanConsumerVertexLatency(0.07);
		edge1.setMeanConsumerVertexLatencyVariance(0.08);
		edge1.setMeanConsumerVertexInterarrivalTimeVariance(2.42);
		edge1.setTransportLatencyMean(6.19);
		return edge1;
	}

	public static QosGroupEdgeSummary getEdge2() {
		QosGroupEdgeSummary edge2 = new QosGroupEdgeSummary();
		edge2.setMeanEmissionRate(7500.03);
		edge2.setActiveEmitterVertices(200);
		edge2.setActiveConsumerVertices(50);
		edge2.setMeanConsumerVertexLatency(0.001);
		edge2.setMeanConsumerVertexLatencyVariance(0);
		edge2.setMeanConsumerVertexInterarrivalTimeVariance(0.01);
		edge2.setTransportLatencyMean(0.42);
		return edge2;
	}

	@Before
	public void setup() {
		edge1 = getEdge1();
		edge2 = getEdge2();
	}

	@Test
	public void testEdge1QueueWait() {
		GG1Server server = new GG1Server(new JobVertexID(), 1, 500, edge1);
		assertClose(server.getKLBQueueWait(200),
				edge1.getTransportLatencyMean() / 1000);
		assertTrue(server.getLowerBoundParallelism() == 106);
		assertTrue(server.getUpperBoundParallelism() == 500);
	}

	@Test
	public void testEdge2QueueWait() {
		GG1Server server = new GG1Server(new JobVertexID(), 1, 200, edge2);
		assertClose(server.getKLBQueueWait(50),
				edge2.getTransportLatencyMean() / 1000);
		assertTrue(server.getLowerBoundParallelism() == 2);
		assertTrue(server.getUpperBoundParallelism() == 200);
	}

	private void assertClose(double gotten, double expected) {
		assertTrue(Math.abs(gotten - expected) / expected < 0.0000001);
	}
}
