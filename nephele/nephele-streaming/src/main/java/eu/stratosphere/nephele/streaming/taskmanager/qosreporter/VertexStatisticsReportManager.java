package eu.stratosphere.nephele.streaming.taskmanager.qosreporter;

import eu.stratosphere.nephele.streaming.SamplingStrategy;
import eu.stratosphere.nephele.streaming.message.qosreport.VertexStatistics;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex.ReadReadReporter;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex.ReadReadVertexQosReporterGroup;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex.ReadWriteReporter;
import eu.stratosphere.nephele.streaming.taskmanager.qosreporter.vertex.VertexQosReporter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Handles the measurement and reporting of latencies and record
 * consumption/emission rates for a particular vertex. Such a latency is defined
 * as the timespan between record receptions and emits on a particular
 * input/output gate combination of the vertex. Thus one vertex may have
 * multiple associated latencies, one for each input/output gate combination.
 * Which gate combination is measured and reported on must be configured by
 * calling {@link #addReporter(int, int, eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID.Vertex,
 * eu.stratosphere.nephele.streaming.SamplingStrategy)}.
 * <p/>
 * An {@link VertexStatistics} record per configured input/output gate
 * combination will be handed to the provided {@link QosReportForwarderThread}
 * approximately once per aggregation interval (see
 * {@link QosReporterConfigCenter}). "Approximately" because if no records have
 * been received/emitted, nothing will be reported.
 *
 * @author Bjoern Lohrmann
 */
public class VertexStatisticsReportManager {

	// private static final Log LOG =
	// LogFactory.getLog(TaskLatencyReporter.class);

	private QosReportForwarderThread reportForwarder;
	private ConcurrentHashMap<QosReporterID, VertexQosReporter> reporters;
	private AtomicReferenceArray<VertexQosReporter[]> reportersByInputGate;
	private AtomicReferenceArray<VertexQosReporter[]> reportersByOutputGate;

	public VertexStatisticsReportManager(QosReportForwarderThread qosReporter,
			int noOfInputGates, int noOfOutputGates) {

		this.reportForwarder = qosReporter;
		this.reportersByInputGate = new AtomicReferenceArray<VertexQosReporter[]>(
				noOfInputGates);
		this.fillWithEmptyArrays(this.reportersByInputGate, noOfInputGates);
		this.reportersByOutputGate = new AtomicReferenceArray<VertexQosReporter[]>(
				noOfOutputGates);
		this.fillWithEmptyArrays(this.reportersByOutputGate, noOfOutputGates);
		this.reporters = new ConcurrentHashMap<QosReporterID, VertexQosReporter>();
	}

	private void fillWithEmptyArrays(
			AtomicReferenceArray<VertexQosReporter[]> reporterArrays,
			int noOfEmptyArrays) {

		VertexQosReporter[] emptyArray = new VertexQosReporter[0];
		for (int i = 0; i < noOfEmptyArrays; i++) {
			reporterArrays.set(i, emptyArray);
		}
	}

	public void recordReceived(int runtimeInputGateIndex) {
		for (VertexQosReporter reporter : this.reportersByInputGate.get(runtimeInputGateIndex)) {
			reporter.recordReceived(runtimeInputGateIndex);
		}
	}

	public void tryingToReadRecord(int runtimeInputGateIndex) {
		for (VertexQosReporter reporter : this.reportersByInputGate.get(runtimeInputGateIndex)) {
			reporter.tryingToReadRecord(runtimeInputGateIndex);
		}
	}

	public void recordEmitted(int runtimeOutputGateIndex) {
		for (VertexQosReporter reporter : this.reportersByOutputGate.get(runtimeOutputGateIndex)) {
			reporter.recordEmitted(runtimeOutputGateIndex);
		}
	}

	public boolean containsReporter(QosReporterID.Vertex reporterID) {
		return this.reporters.containsKey(reporterID);
	}

	public synchronized void addReporter(int runtimeInputGateIndex,
			int runtimeOutputGateIndex, QosReporterID.Vertex reporterID, SamplingStrategy samplingStrategy) {

		if (this.reporters.containsKey(reporterID)) {
			return;
		}

		VertexQosReporter reporter;

		switch (samplingStrategy) {
			case READ_WRITE:
				reporter = new ReadWriteReporter(reportForwarder, reporterID, runtimeInputGateIndex, runtimeOutputGateIndex);
				// for the READ_READ as well as READ_WRITE strategy the reporter needs to keep track of all input gates
				for (int i = 0; i < this.reportersByInputGate.length(); i++) {
					appendReporterToArrayAt(reportersByInputGate, i, reporter);
				}
				// for the READ_WRITE strategy the reporter needs to keep track of all output gates, too
				for (int i = 0; i < this.reportersByOutputGate.length(); i++) {
					appendReporterToArrayAt(reportersByOutputGate, i, reporter);
				}
				break;

			case READ_READ:
			default:
				reporter = new ReadReadReporter(reportForwarder, reporterID, runtimeInputGateIndex, runtimeOutputGateIndex);

				// get or create ReadReadVertexQosReporterGroup
				ReadReadVertexQosReporterGroup groupReporter = null;
				for (VertexQosReporter vertexQosReporter : reportersByInputGate.get(runtimeInputGateIndex)) {
					if (vertexQosReporter instanceof ReadReadVertexQosReporterGroup
							&& vertexQosReporter.getRuntimeInputGateIndex() == runtimeInputGateIndex) {
						groupReporter = (ReadReadVertexQosReporterGroup) vertexQosReporter;
						break;
					}
				}

				// create a ReadReadVertexQosReporterGroup if none found for the given runtimeInputGateIndex
				if (groupReporter == null) {
					groupReporter = new ReadReadVertexQosReporterGroup(null, null, runtimeInputGateIndex, -1);
					// for the READ_READ as well as READ_WRITE strategy the reporter needs to keep track of all input gates
					for (int i = 0; i < this.reportersByInputGate.length(); i++) {
						appendReporterToArrayAt(reportersByInputGate, i, groupReporter);
					}
				}

				groupReporter.addReporter((ReadReadReporter) reporter);
		}

		this.reporters.put(reporterID, reporter);
	}

	private void appendReporterToArrayAt(AtomicReferenceArray<VertexQosReporter[]> reporters, int index,
			VertexQosReporter reporterToAppend) {
		VertexQosReporter[] oldReporters = reporters.get(index);
		VertexQosReporter[] newReporters = new VertexQosReporter[oldReporters.length + 1];
		System.arraycopy(oldReporters, 0, newReporters, 0, oldReporters.length);
		newReporters[oldReporters.length] = reporterToAppend;
		reporters.set(index, newReporters);
	}
}
