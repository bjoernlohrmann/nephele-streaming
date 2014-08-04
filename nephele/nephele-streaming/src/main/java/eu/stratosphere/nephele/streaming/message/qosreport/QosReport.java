package eu.stratosphere.nephele.streaming.message.qosreport;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.streaming.message.AbstractSerializableQosMessage;
import eu.stratosphere.nephele.streaming.message.action.EdgeQosReporterConfig;
import eu.stratosphere.nephele.streaming.message.action.VertexQosReporterConfig;
import eu.stratosphere.nephele.streaming.taskmanager.qosmodel.QosReporterID;

/**
 * Holds Qos report data to be shipped to a specific Qos manager. Instead of
 * sending each {@link AbstractQosReportRecord} individually, they are sent in
 * batch. Most internal fields of this class are initialized in a lazy fashion,
 * thus (empty) instances of this class have a small memory footprint.
 * 
 * @author Bjoern Lohrmann
 */
public class QosReport extends AbstractSerializableQosMessage {

	private HashMap<QosReporterID.Edge, EdgeLatency> edgeLatencies;

	private HashMap<QosReporterID.Edge, EdgeStatistics> edgeStatistics;

	private HashMap<QosReporterID.Vertex, VertexStatistics> vertexStatistics;

	private LinkedList<VertexQosReporterConfig> vertexReporterAnnouncements;

	private LinkedList<EdgeQosReporterConfig> edgeReporterAnnouncements;

	/**
	 * Creates and initializes QosReport object to be used for
	 * sending/serialization.
	 * 
	 * @param jobID
	 */
	public QosReport(JobID jobID) {
		super(jobID);
	}

	/**
	 * Creates and initializes QosReport object to be used for
	 * receiving/deserialization.
	 */
	public QosReport() {
		super();
	}

	private HashMap<QosReporterID.Edge, EdgeLatency> getOrCreateEdgeLatencyMap() {
		if (this.edgeLatencies == null) {
			this.edgeLatencies = new HashMap<QosReporterID.Edge, EdgeLatency>();
		}
		return this.edgeLatencies;
	}

	private HashMap<QosReporterID.Edge, EdgeStatistics> getOrCreateEdgeStatisticsMap() {
		if (this.edgeStatistics == null) {
			this.edgeStatistics = new HashMap<QosReporterID.Edge, EdgeStatistics>();
		}
		return this.edgeStatistics;
	}

	private HashMap<QosReporterID.Vertex, VertexStatistics> getOrCreateVertexStatisticsMap() {
		if (this.vertexStatistics == null) {
			this.vertexStatistics = new HashMap<QosReporterID.Vertex, VertexStatistics>();
		}
		return this.vertexStatistics;
	}

	public void addEdgeLatency(EdgeLatency edgeLatency) {
		QosReporterID.Edge reporterID = edgeLatency.getReporterID();

		EdgeLatency existing = this.getOrCreateEdgeLatencyMap().get(reporterID);
		if (existing == null) {
			this.getOrCreateEdgeLatencyMap().put(reporterID, edgeLatency);
		} else {
			existing.add(edgeLatency);
		}
	}

	public void announceVertexQosReporter(VertexQosReporterConfig vertexReporter) {
		if (this.vertexReporterAnnouncements == null) {
			this.vertexReporterAnnouncements = new LinkedList<VertexQosReporterConfig>();
		}
		this.vertexReporterAnnouncements.add(vertexReporter);
	}

	public List<VertexQosReporterConfig> getVertexQosReporterAnnouncements() {
		if (this.vertexReporterAnnouncements == null) {
			return Collections.emptyList();
		}
		return this.vertexReporterAnnouncements;
	}

	public void addEdgeQosReporterAnnouncement(
			EdgeQosReporterConfig edgeReporter) {
		if (this.edgeReporterAnnouncements == null) {
			this.edgeReporterAnnouncements = new LinkedList<EdgeQosReporterConfig>();
		}
		this.edgeReporterAnnouncements.add(edgeReporter);
	}

	public List<EdgeQosReporterConfig> getEdgeQosReporterAnnouncements() {
		if (this.edgeReporterAnnouncements == null) {
			return Collections.emptyList();
		}
		return this.edgeReporterAnnouncements;
	}

	public Collection<EdgeLatency> getEdgeLatencies() {
		if (this.edgeLatencies == null) {
			return Collections.emptyList();
		}
		return this.edgeLatencies.values();
	}

	public void addEdgeStatistics(EdgeStatistics edgeStats) {

		QosReporterID.Edge reporterID = edgeStats.getReporterID();

		EdgeStatistics existing = this.getOrCreateEdgeStatisticsMap().get(
				edgeStats);
		if (existing == null) {
			this.getOrCreateEdgeStatisticsMap().put(reporterID, edgeStats);
		} else {
			existing.add(edgeStats);
		}
	}

	public Collection<EdgeStatistics> getEdgeStatistics() {
		if (this.edgeStatistics == null) {
			return Collections.emptyList();
		}
		return this.edgeStatistics.values();
	}

	public void addVertexStatistics(VertexStatistics vertexStats) {
		QosReporterID.Vertex reporterID = vertexStats.getReporterID();
		VertexStatistics existing = this.getOrCreateVertexStatisticsMap().get(
				reporterID);
		if (existing == null) {
			this.getOrCreateVertexStatisticsMap().put(reporterID, vertexStats);
		} else {
			existing.add(vertexStats);
		}
	}

	public Collection<VertexStatistics> getVertexStatistics() {
		if (this.vertexStatistics == null) {
			return Collections.emptyList();
		}
		return this.vertexStatistics.values();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		super.write(out);
		this.writeEdgeLatencies(out);
		this.writeEdgeStatistics(out);
		this.writeVertexLatencies(out);
		this.writeVertexReporterAnnouncements(out);
		this.writeEdgeReporterAnnouncements(out);
	}

	private void writeEdgeReporterAnnouncements(DataOutput out)
			throws IOException {
		if (this.edgeReporterAnnouncements != null) {
			out.writeInt(this.edgeReporterAnnouncements.size());
			for (EdgeQosReporterConfig reporterConfig : this.edgeReporterAnnouncements) {
				reporterConfig.write(out);
			}
		} else {
			out.writeInt(0);
		}
	}

	private void writeVertexReporterAnnouncements(DataOutput out)
			throws IOException {
		if (this.vertexReporterAnnouncements != null) {
			out.writeInt(this.vertexReporterAnnouncements.size());
			for (VertexQosReporterConfig reporterConfig : this.vertexReporterAnnouncements) {
				reporterConfig.write(out);
			}
		} else {
			out.writeInt(0);
		}
	}

	private void writeEdgeLatencies(DataOutput out) throws IOException {
		if (this.edgeLatencies != null) {
			out.writeInt(this.edgeLatencies.size());
			for (Entry<QosReporterID.Edge, EdgeLatency> entry : this.edgeLatencies
					.entrySet()) {
				entry.getKey().write(out);
				out.writeDouble(entry.getValue().getEdgeLatency());
			}
		} else {
			out.writeInt(0);
		}
	}

	private void writeEdgeStatistics(DataOutput out) throws IOException {
		if (this.edgeStatistics != null) {
			out.writeInt(this.edgeStatistics.size());
			for (Entry<QosReporterID.Edge, EdgeStatistics> entry : this.edgeStatistics
					.entrySet()) {
				entry.getKey().write(out);
				out.writeDouble(entry.getValue().getThroughput());
				out.writeDouble(entry.getValue().getOutputBufferLifetime());
				out.writeDouble(entry.getValue().getRecordsPerBuffer());
				out.writeDouble(entry.getValue().getRecordsPerSecond());
			}
		} else {
			out.writeInt(0);
		}
	}

	private void writeVertexLatencies(DataOutput out) throws IOException {
		if (this.vertexStatistics != null) {
			out.writeInt(this.vertexStatistics.size());
			for (VertexStatistics vertexStat: this.vertexStatistics.values()) {
				vertexStat.write(out);
			}
		} else {
			out.writeInt(0);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {
		super.read(in);
		this.readEdgeLatencies(in);
		this.readOutputEdgeStatistics(in);
		this.readVertexStatistics(in);
		this.readVertexReporterAnnouncements(in);
		this.readEdgeReporterAnnouncements(in);
	}

	private void readVertexReporterAnnouncements(DataInput in)
			throws IOException {
		this.vertexReporterAnnouncements = new LinkedList<VertexQosReporterConfig>();
		int toRead = in.readInt();
		for (int i = 0; i < toRead; i++) {
			VertexQosReporterConfig reporterConfig = new VertexQosReporterConfig();
			reporterConfig.read(in);
			this.vertexReporterAnnouncements.add(reporterConfig);
		}
	}

	private void readEdgeReporterAnnouncements(DataInput in) throws IOException {
		this.edgeReporterAnnouncements = new LinkedList<EdgeQosReporterConfig>();
		int toRead = in.readInt();
		for (int i = 0; i < toRead; i++) {
			EdgeQosReporterConfig reporterConfig = new EdgeQosReporterConfig();
			reporterConfig.read(in);
			this.edgeReporterAnnouncements.add(reporterConfig);
		}
	}

	private void readEdgeLatencies(DataInput in) throws IOException {
		int toRead = in.readInt();
		for (int i = 0; i < toRead; i++) {
			QosReporterID.Edge reporterID = new QosReporterID.Edge();
			reporterID.read(in);

			EdgeLatency edgeLatency = new EdgeLatency(reporterID,
					in.readDouble());
			this.getOrCreateEdgeLatencyMap().put(reporterID, edgeLatency);
		}
	}

	private void readOutputEdgeStatistics(DataInput in) throws IOException {
		int toRead = in.readInt();
		for (int i = 0; i < toRead; i++) {
			QosReporterID.Edge reporterID = new QosReporterID.Edge();
			reporterID.read(in);

			EdgeStatistics edgeStats = new EdgeStatistics(reporterID,
					in.readDouble(), in.readDouble(), in.readDouble(),
					in.readDouble());
			this.getOrCreateEdgeStatisticsMap().put(reporterID, edgeStats);
		}
	}

	private void readVertexStatistics(DataInput in) throws IOException {
		int toRead = in.readInt();
		for (int i = 0; i < toRead; i++) {
			VertexStatistics vertexStat = new VertexStatistics();
			vertexStat.read(in);
			this.getOrCreateVertexStatisticsMap().put(vertexStat.getReporterID(), vertexStat);
		}
	}

	public boolean isEmpty() {
		return this.edgeLatencies == null && this.edgeStatistics == null
				&& this.vertexStatistics == null
				&& this.vertexReporterAnnouncements == null
				&& this.edgeReporterAnnouncements == null;
	}

	public boolean hasAnnouncements() {
		return this.vertexReporterAnnouncements != null
				|| this.edgeReporterAnnouncements != null;
	}
}
