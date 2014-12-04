package eu.stratosphere.nephele.streaming.jobmanager.autoscaling.optimization;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosGroupEdgeSummary;

public class GG1ServerTest {

	private static QosGroupEdgeSummary edge1 = getEdge1();
	private static QosGroupEdgeSummary edge2 = getEdge2();
	private static QosGroupEdgeSummary edge3 = getEdge3();

	public static QosGroupEdgeSummary getEdge1() {
		QosGroupEdgeSummary edge1 = new QosGroupEdgeSummary();
		edge1.setMeanEmissionRate(7500);
		edge1.setActiveEmitterVertices(50);
		edge1.setActiveConsumerVertices(50);
		edge1.setMeanConsumerVertexLatency(0.08);
		edge1.setMeanConsumerVertexLatencyCV(0.3535534);
		edge1.setMeanConsumerVertexInterarrivalTimeCV(11.66726);
		edge1.setTransportLatencyMean(8);
		return edge1;
	}

	public static QosGroupEdgeSummary getEdge2() {
		QosGroupEdgeSummary edge2 = new QosGroupEdgeSummary();
		edge2.setMeanEmissionRate(7500);
		edge2.setActiveEmitterVertices(75);
		edge2.setActiveConsumerVertices(50);
		edge2.setMeanConsumerVertexLatency(0.04);
		edge2.setMeanConsumerVertexLatencyCV(2.5);
		edge2.setMeanConsumerVertexInterarrivalTimeCV(13.77838);
		edge2.setTransportLatencyMean(3);
		return edge2;
	}
	
	public static QosGroupEdgeSummary getEdge3() {
		QosGroupEdgeSummary edge3 = new QosGroupEdgeSummary();
		edge3.setMeanEmissionRate(7500);
		edge3.setActiveEmitterVertices(60);
		edge3.setActiveConsumerVertices(50);
		edge3.setMeanConsumerVertexLatency(0.09);
		edge3.setMeanConsumerVertexLatencyCV(0.03513642);
		edge3.setMeanConsumerVertexInterarrivalTimeCV(6.363961);
		edge3.setTransportLatencyMean(7);
		return edge3;
	}

	@Before
	public void setup() {
		edge1 = getEdge1();
		edge2 = getEdge2();
	}

	@Test
	public void testEdge1QueueWait() {
		GG1Server server = createKingmanEdge1();
		assertClose(server.getQueueWait(edge1.getActiveConsumerVertices()),
				edge1.getTransportLatencyMean() / 1000);
		assertEquals(31, server.getLowerBoundParallelism());
		assertEquals(70, server.getUpperBoundParallelism());
		assertClose(server.getQueueWait(40), 0.016);
		assertClose(server.getQueueWait(70), 0.004);
	}
	
	

	@Test
	public void testEdge2QueueWait() {
		GG1Server server = createKingmanEdge2();
		assertClose(server.getQueueWait(edge2.getActiveConsumerVertices()),
				edge2.getTransportLatencyMean() / 1000);
		assertEquals(23, server.getLowerBoundParallelism());
		assertEquals(70, server.getUpperBoundParallelism());
		assertClose(server.getQueueWait(40), 0.004714286);
		assertClose(server.getQueueWait(70), 0.001736842);
	}

	@Test
	public void testStrictlyDecreasingQueueWaitKingman() {
		GG1Server server = createKingmanEdge1();

		double lastWait = Double.POSITIVE_INFINITY;
		for (int i = server.getLowerBoundParallelism(); i <= server
				.getUpperBoundParallelism(); i++) {
			double thisWait = server.getQueueWait(i);
			assertTrue(lastWait > thisWait);
			lastWait = thisWait;
		}
	}

	@Test
	public void testStrictlyDecreasingQueueWaitKLB() {
		GG1Server server = createKLBEdge1();

		double lastWait = Double.POSITIVE_INFINITY;
		for (int i = server.getLowerBoundParallelism(); i <= server
				.getUpperBoundParallelism(); i++) {
			double thisWait = server.getQueueWait(i);
			assertTrue(lastWait > thisWait);
			lastWait = thisWait;
		}
	}

	@Test
	public void testComputeQueueWaitGradient() {
		List<GG1Server> servers = Arrays.asList(
				(GG1Server) createKingmanEdge1(), createKingmanEdge2(),
				createKLBEdge1(), createKLBEdge2());

		for (GG1Server server : servers) {
			double lastGradient = Double.NEGATIVE_INFINITY;
			for (int i = server.getLowerBoundParallelism(); i < server
					.getUpperBoundParallelism(); i++) {
				double thisWait = server.getQueueWait(i);
				double nextWait = server.getQueueWait(i + 1);
				double thisGradient = nextWait - thisWait;
				assertClose(server.computeQueueWaitGradient(i), thisGradient);
				assertTrue(thisGradient > lastGradient);
				lastGradient = thisGradient;
			}
		}
	}

	@Test
	public void testComputeParallelismForGradientThreshold() {
		List<GG1Server> servers = Arrays.asList(
				(GG1Server) createKingmanEdge1(), createKingmanEdge2(),
				createKLBEdge1(), createKLBEdge2());

		for (GG1Server server : servers) {
			for (int i = server.getLowerBoundParallelism(); i < server.getUpperBoundParallelism(); i++) {
				double thisGradient = server.computeQueueWaitGradient(i);
				
				int p = server.computeParallelismForGradientThreshold(thisGradient);
				assertEquals(i, p);
			}
		}
	}

	public static GG1ServerKLB createKLBEdge1() {
		return new GG1ServerKLB(new JobVertexID(), 1, 70, edge1);
	}

	public static GG1ServerKLB createKLBEdge2() {
		return new GG1ServerKLB(new JobVertexID(), 1, 70, edge2);
	}
	
	public static GG1ServerKLB createKLBEdge3() {
		return new GG1ServerKLB(new JobVertexID(), 1, 70, edge3);
	}

	public static GG1ServerKingman createKingmanEdge1() {
		return new GG1ServerKingman(new JobVertexID(), 1, 70, edge1);
	}

	public static GG1ServerKingman createKingmanEdge2() {
		return new GG1ServerKingman(new JobVertexID(), 1, 70, edge2);
	}
	
	public static GG1ServerKingman createKingmanEdge3() {
		return new GG1ServerKingman(new JobVertexID(), 1, 70, edge3);
	}

	private void assertClose(double gotten, double expected) {
		assertTrue(Math.abs(gotten - expected) / expected < 0.0000001);
	}
}
