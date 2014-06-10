package eu.stratosphere.nephele.template;

import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.types.Record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Collector<T extends Record> {
  private boolean flush = false;
  private List<T> buffer = new ArrayList<T>();
  private RecordWriter<T> writer;

  public Collector(RecordWriter<T> writer) {
    this.writer = writer;
  }

  public void emit(T record) {
    buffer.add(record);
  }

  public void flush() {
    flush = true;
  }

  public void flushBuffer() throws IOException, InterruptedException {
    for (T record : buffer) {
      writer.emit(record);
    }
    buffer.clear();
    if (flush) {
      writer.flush();
      flush = false;
    }
  }

  public RecordWriter<T> getRecordWriter() {
    return writer;
  }
}
