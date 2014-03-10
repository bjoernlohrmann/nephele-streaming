package eu.stratosphere.nephele.template;

import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.types.Record;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;


public abstract class IoCTask extends AbstractTask {
  private ArrayList<RecordReader<? extends Record>> readers = new ArrayList<RecordReader<? extends Record>>();
  private ArrayList<RecordWriter<? extends Record>> writers = new ArrayList<RecordWriter<? extends Record>>();
  private ArrayList<int[]> mappings = new ArrayList<int[]>();
  private ArrayList<Method> methods = new ArrayList<Method>();


  @Override
  public void registerInputOutput() {

    // expects the user to call initReader and/or initWriter
    setup();

    //initialize data structures
    for (int i = 0; i < readers.size(); i++) {
      methods.add(null);
      mappings.add(null);
    }

    Method[] methods = this.getClass().getMethods();
    for (Method method : methods) {
      ReadFromWriteTo annotation = method.getAnnotation(ReadFromWriteTo.class);
      if (annotation == null) {
        continue;
      }

      int readerIndex = annotation.readerIndex();
      int[] writerIndices = annotation.writerIndices();

      this.mappings.set(readerIndex, writerIndices);
      this.methods.set(readerIndex, method);
    }

    // validation
    validate();
  }

  private void validate() {
    assert !methods.contains(null);
    assert !mappings.contains(null);
    //TODO check that all writers are used
  }


  protected abstract void setup();

  protected <T extends Record> void initReader(int index, Class<T> recordType) {
    readers.add(index, new RecordReader<T>(this, recordType));
  }

  protected <T extends Record> void initWriter(int index, Class<T> recordType) {
    writers.add(index, new RecordWriter<T>(this, recordType));
  }

  private Object[] getArguments(int readerIndex) throws IOException, InterruptedException {
    RecordReader<? extends Record> reader = readers.get(readerIndex);
    int[] writerIndices = mappings.get(readerIndex);
    Object[] args = new Object[writerIndices.length+1];
    int i = 0;
    args[i++] = reader.next();
    for (int writerIndex : writerIndices) {
      args[i++] = writers.get(writerIndex);
    }
    return args;
  }

  @Override
  public void invoke() throws Exception {

    while (readers.size() > 0) {


      for (int i = 0; i < readers.size(); i++) {
        RecordReader<? extends Record> reader = readers.get(i);
        switch (reader.hasNextNonBlocking()) {
          case RECORD_AVAILABLE:
            Object[] args = getArguments(i);
            methods.get(i).invoke(this, args);
            break;
          case NONE:
            //TODO
            break;
          case END_OF_STREAM:
            readers.remove(i);
            mappings.remove(i);
            methods.remove(i);
            i--;
            break;
          default:
            break;
        }
      }


    }

  }
}
