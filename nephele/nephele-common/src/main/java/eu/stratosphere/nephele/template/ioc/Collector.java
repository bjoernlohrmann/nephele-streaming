package eu.stratosphere.nephele.template.ioc;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.types.Record;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;


public class Collector<T extends Record> {
	private boolean flush = false;
	private Queue<T> buffer = new ArrayDeque<T>();
	private RecordWriter<T> writer;

	public Collector(RecordWriter<T> writer) {
		this.writer = writer;
	}

	public void collect(T record) {
		buffer.add(record);
	}

	public void flush() {
		flush = true;
	}

	public void flushBuffer() throws IOException, InterruptedException {
		T record;
		while ((record = buffer.poll()) != null) {
			writer.emit(record);
		}
		if (flush) {
			writer.flush();
			flush = false;
		}
	}

	public RecordWriter<T> getRecordWriter() {
		return writer;
	}
}
