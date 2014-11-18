package eu.stratosphere.nephele.streaming.jobmanager.autoscaling.optimization;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

import eu.stratosphere.nephele.jobgraph.JobVertexID;
import eu.stratosphere.nephele.streaming.taskmanager.qosmanager.QosGroupEdgeSummary;

public class RebalancerTest {
	
	@Test
	public void testRebalanceWith2Edges() {
		ArrayList<GG1Server> servers = new ArrayList<GG1Server>();
		servers.add(new GG1Server(new JobVertexID(), 1, 500, GG1ServerTest.getEdge1()));
		servers.add(new GG1Server(new JobVertexID(), 1, 100, GG1ServerTest.getEdge2()));
		
		Rebalancer reb = new Rebalancer(servers, 4);
		assertTrue(reb.computeRebalancedParallelism());
		assertSolutionIsCompleteAndValid(servers, reb);
		
		int resultCost = reb.getRebalancedParallelismCost();
		assertTrue(resultCost == 279);
	}
	
	private void assertSolutionIsCompleteAndValid(ArrayList<GG1Server> servers,
			Rebalancer reb) {
		
		Map<JobVertexID, Integer> result = reb.getRebalancedParallelism();
		assertTrue(result.size() == servers.size());
		for (GG1Server server : servers) {
			JobVertexID id = server.getGroupVertexID();
			assertTrue(server.getLowerBoundParallelism() <= result.get(id));
			assertTrue(result.get(id) <= server.getUpperBoundParallelism());
			assertTrue(server.getMeanUtilization(result.get(id)) < 1);
		}
	}

	@Test
	public void testRebalanceWith10Edges() {
		Random rnd = new Random(234627834);
		
		for (int i = 0; i < 10; i++) {
			ArrayList<GG1Server> servers = create10RandomizedServers(rnd);
			Rebalancer reb = new Rebalancer(servers, 10);
			assertTrue(reb.computeRebalancedParallelism());
			assertSolutionIsCompleteAndValid(servers, reb);
		}
	}
	
	private ArrayList<GG1Server> create10RandomizedServers(Random rnd) {
		
		ArrayList<GG1Server> servers = new ArrayList<GG1Server>();
		
		for(int i=0; i< 9; i++) {
			QosGroupEdgeSummary edgeSum = GG1ServerTest.getEdge1();
			
			double serverUtil = 0.2 + 0.7 * rnd.nextDouble();
			
			double lambdaTotal = edgeSum.getMeanEmissionRate() * edgeSum.getActiveEmitterVertices() * rnd.nextDouble();
			double s = edgeSum.getMeanConsumerVertexLatency() * 2 * rnd.nextDouble();
			double varS = edgeSum.getMeanConsumerVertexLatencyVariance() * 2 * rnd.nextDouble();
			double varA = edgeSum.getMeanConsumerVertexInterarrivalTimeVariance() * 2 * rnd.nextDouble();
			int maxP = 1000;
			int p = (int) Math.ceil(lambdaTotal * (s / 1000) / serverUtil);
			double queueWait = (1 + (9*rnd.nextDouble()));
			
			edgeSum.setMeanEmissionRate(lambdaTotal / edgeSum.getActiveEmitterVertices());
			edgeSum.setMeanConsumerVertexLatency(s);
			edgeSum.setMeanConsumerVertexLatencyVariance(varS);
			edgeSum.setMeanConsumerVertexInterarrivalTimeVariance(varA);
			edgeSum.setActiveConsumerVertices(p);
			edgeSum.setTransportLatencyMean(queueWait);
			
			servers.add(new GG1Server(new JobVertexID(), 1, maxP, edgeSum));
		}
		
		servers.add(new GG1Server(new JobVertexID(), 1, 100, GG1ServerTest.getEdge2()));
		return servers;
	}
	
	@Test
	public void testImpossibleRebalance() {
		ArrayList<GG1Server> servers = new ArrayList<GG1Server>();
		servers.add(new GG1Server(new JobVertexID(), 1, 210, GG1ServerTest.getEdge1()));
		servers.add(new GG1Server(new JobVertexID(), 1, 100, GG1ServerTest.getEdge2()));
		
		Rebalancer reb = new Rebalancer(servers, 4);
		assertFalse(reb.computeRebalancedParallelism());
		assertSolutionIsCompleteAndValid(servers, reb);
		
		int resultCost = reb.getRebalancedParallelismCost();
		assertTrue(resultCost == 210+100);
		
		Map<JobVertexID, Integer> result = reb.getRebalancedParallelism();
		assertTrue(result.size() == servers.size());
		for (GG1Server server : servers) {
			JobVertexID id = server.getGroupVertexID();
			assertTrue(server.getLowerBoundParallelism() <= result.get(id));
			assertTrue(result.get(id) <= server.getUpperBoundParallelism());
		}
	}
}
