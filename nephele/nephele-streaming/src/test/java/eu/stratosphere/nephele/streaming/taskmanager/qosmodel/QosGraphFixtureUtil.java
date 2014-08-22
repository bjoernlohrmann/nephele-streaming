package eu.stratosphere.nephele.streaming.taskmanager.qosmodel;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.powermock.api.mockito.PowerMockito;

import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;

public class QosGraphFixtureUtil {
    static InstanceConnectionInfo[] generateAndAssignInstances(
            ExecutionGroupVertex groupVertex) {

        InstanceConnectionInfo[] connectionInfos = new InstanceConnectionInfo[groupVertex
                .getCurrentNumberOfGroupMembers()];
        for (int i = 0; i < connectionInfos.length; i++) {
            ExecutionVertex vertex = groupVertex.getGroupMember(i);

            try {
                connectionInfos[i] = new InstanceConnectionInfo(
                        InetAddress.getByName(String.format("10.10.10.%d",
                                i + 1)), "hostname", "domainname", 1, 1);

                AbstractInstance instance = PowerMockito.mock(AbstractInstance.class);
                PowerMockito.when(instance.getInstanceConnectionInfo()).thenReturn(
                        connectionInfos[i]);

                AllocatedResource resource = PowerMockito.mock(AllocatedResource.class);
                PowerMockito.when(resource.getInstance()).thenReturn(instance);

                vertex.setAllocatedResource(resource);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        return connectionInfos;
    }
}