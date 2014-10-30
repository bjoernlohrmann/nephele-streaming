package eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex;

import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.QosReportForwarderThread;

public interface ReportSendingStrategy {
	public void sendReport(QosReportForwarderThread reportForwarder, double avgSampleTime,
			double consumptionRate, double emissionRate);
}
