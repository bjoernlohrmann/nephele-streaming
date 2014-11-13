package eu.stratosphere.nephele.streaming.taskmanager.qosmanager;

import java.util.List;

import eu.stratosphere.nephele.io.IOReadableWritable;

public interface QosGroupElementSummary extends IOReadableWritable {
	
	public boolean isVertex();
	
	public boolean isEdge();
	
	public boolean hasData();

	public void merge(List<QosGroupElementSummary> elemSummaries);
}
