package eu.stratosphere.nephele.streaming.jobmanager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.collections.Sets;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionSignature;
import eu.stratosphere.nephele.executiongraph.ExecutionStage;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.streaming.StreamingPluginLoader;
import eu.stratosphere.nephele.streaming.message.action.DeployInstanceQosRolesAction;

/**
 * Tests concerning the class {@link QosSetup}
 *
 */
@PrepareForTest({ ExecutionSignature.class, AbstractInstance.class,
		AllocatedResource.class })
@RunWith(PowerMockRunner.class)
public class QosSetupTest {
	private QosGraphFixtureMultiInstanceConnections fix;

	@Before
	public void setUp() throws Exception {
		this.fix = new QosGraphFixtureMultiInstanceConnections();
	}

	@Test
	public void testFixture() throws Exception {
		assertEquals(2,
				this.fix.execInputVertex.getCurrentNumberOfGroupMembers());
		assertEquals(2,
				this.fix.execTaskVertex.getCurrentNumberOfGroupMembers());
		assertEquals(2,
				this.fix.execOutputVertex.getCurrentNumberOfGroupMembers());

		this.assertNonEqualMembers(this.fix.execInputVertex);
		this.assertNonEqualMembers(this.fix.execTaskVertex);
		this.assertNonEqualMembers(this.fix.execOutputVertex);

		assertEquals(1, this.fix.constraints.size());

	}

	private void assertNonEqualMembers(ExecutionGroupVertex groupVertex) {
		for (int i = 0; i < groupVertex.getCurrentNumberOfGroupMembers() - 1; i++) {
			ExecutionVertex currentMember = groupVertex.getGroupMember(i);
			ExecutionVertex nextMember = groupVertex.getGroupMember(i + 1);
			assertFalse(currentMember.getAllocatedResource().equals(
					nextMember.getAllocatedResource()));
		}
	}

	/**
	 * This method will iterate over the
	 * {@link QosGraphFixtureMultiInstanceConnections#executionGraph}'s vertices
	 * and at stage one find all {@link DeployInstanceQosRolesAction}s.
	 * 
	 * At stage two we check if these are actually the instances that we
	 * exepcted.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testManagerAssignment() throws Exception {
		QosSetup qosSetup = new QosSetup(this.fix.constraints);
		qosSetup.computeQosRoles();
		qosSetup.attachRolesToExecutionGraph(this.fix.executionGraph);
		ExecutionStage stage = this.fix.executionGraph.getStage(0);
		Map<ExecutionVertex, DeployInstanceQosRolesAction> vertexToRole = new HashMap<ExecutionVertex, DeployInstanceQosRolesAction>();

		for (int i = 0; i < stage.getNumberOfStageMembers(); i++) {
			ExecutionGroupVertex executionGroupVertex = stage.getStageMember(i);
			for (int j = 0; j < executionGroupVertex
					.getCurrentNumberOfGroupMembers(); j++) {
				ExecutionVertex groupMember = executionGroupVertex
						.getGroupMember(j);

				DeployInstanceQosRolesAction pluginData = (DeployInstanceQosRolesAction) groupMember
						.getPluginData(StreamingPluginLoader.STREAMING_PLUGIN_ID);
				if (pluginData != null)
					vertexToRole.put(groupMember, pluginData);
			}
		}
		// we expect 2 manager nodes
		assertEquals(2, vertexToRole.size());

		ExecutionVertex groupMember1 = this.fix.execTaskVertex
				.getGroupMember(0);
		ExecutionVertex groupMember2 = this.fix.execTaskVertex
				.getGroupMember(1);
		Set<InstanceConnectionInfo> expectedInfos = Sets.newSet(
				this.getInstanceConnectionInfo(groupMember1),
				this.getInstanceConnectionInfo(groupMember2));
		for (Map.Entry<ExecutionVertex, DeployInstanceQosRolesAction> e : vertexToRole
				.entrySet())
			assertTrue(expectedInfos.remove(e.getValue()
					.getInstanceConnectionInfo()));
		assertEquals(0, expectedInfos.size());
	}

	/**
	 * This test works like {@link #testManagerAssignment()} using
	 * {@link QosGraphFixtureMultiInstanceConnections#executionGraph2}.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testManagerAssignmentGraph2() throws Exception {
		QosSetup qosSetup = new QosSetup(this.fix.constraints2);
		qosSetup.computeQosRoles();
		qosSetup.attachRolesToExecutionGraph(this.fix.executionGraph2);
		ExecutionStage stage = this.fix.executionGraph2.getStage(0);
		Map<ExecutionVertex, DeployInstanceQosRolesAction> vertexToRole = new HashMap<ExecutionVertex, DeployInstanceQosRolesAction>();

		for (int i = 0; i < stage.getNumberOfStageMembers(); i++) {
			ExecutionGroupVertex executionGroupVertex = stage.getStageMember(i);
			for (int j = 0; j < executionGroupVertex
					.getCurrentNumberOfGroupMembers(); j++) {
				ExecutionVertex groupMember = executionGroupVertex
						.getGroupMember(j);
				DeployInstanceQosRolesAction pluginData = (DeployInstanceQosRolesAction) groupMember
						.getPluginData(StreamingPluginLoader.STREAMING_PLUGIN_ID);
				if (pluginData != null)
					vertexToRole.put(groupMember, pluginData);
			}
		}

		// we still expect 2 manager nodes
		assertEquals(2, vertexToRole.size());

		ExecutionVertex member1 = this.fix.execTaskVertex2.getGroupMember(0);
		ExecutionVertex member2 = this.fix.execTaskVertex2.getGroupMember(1);

		Set<InstanceConnectionInfo> expectedInfos = Sets.newSet(
				this.getInstanceConnectionInfo(member1),
				this.getInstanceConnectionInfo(member2));
		for (Map.Entry<ExecutionVertex, DeployInstanceQosRolesAction> e : vertexToRole
				.entrySet())
			assertTrue(expectedInfos.remove(e.getValue()
					.getInstanceConnectionInfo()));
		assertEquals(0, expectedInfos.size());
	}

	/**
	 * Shortcut to retrieve the {@link InstanceConnectionInfo} from an
	 * {@link ExecutionVertex}
	 * 
	 * @param executionVertex
	 *            the vertex in question
	 * @return the {@link ExecutionVertex}'s {@link InstanceConnectionInfo}
	 */
	private InstanceConnectionInfo getInstanceConnectionInfo(
			ExecutionVertex executionVertex) {
		return executionVertex.getAllocatedResource().getInstance()
				.getInstanceConnectionInfo();
	}
}
