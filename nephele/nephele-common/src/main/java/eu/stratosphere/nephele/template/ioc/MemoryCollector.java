package eu.stratosphere.nephele.template.ioc;

import eu.stratosphere.nephele.types.Record;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

public class MemoryCollector<T extends Record> extends Collector<T> {
	private Queue<T> buffer = new ArrayDeque<T>();

	public MemoryCollector() {
		super(null);
	}

	@Override
	public void collect(T record) {
		buffer.add(record);
	}

	@Override
	public void flushBuffer() throws IOException, InterruptedException {
	}

	public Queue<T> getBuffer() {
		return buffer;
	}
}
