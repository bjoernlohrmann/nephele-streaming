package eu.stratosphere.nephele.template.ioc;

import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.RecordAvailabilityListener;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.template.IllegalConfigurationException;
import eu.stratosphere.nephele.types.Record;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/**
 * Abstract base class for tasks that can declare user methods which are then called when input is available.
 */
public abstract class IocTask extends AbstractTask {
	private List<RecordReader<? extends Record>> readers = new ArrayList<RecordReader<? extends Record>>();
	private List<Class<? extends Record>> readerRecordTypes = new ArrayList<Class<? extends Record>>();
	private List<Collector<? extends Record>> collectors = new ArrayList<Collector<? extends Record>>();
	private List<Method> methods = new ArrayList<Method>();
	private List<int[]> mappings = new ArrayList<int[]>();
	private List<Method> finishMethods = new ArrayList<Method>();
	private List<int[]> finishMappings = new ArrayList<int[]>();
	private final Set<Integer> availableReaders = new LinkedHashSet<Integer>();
	private Set<Integer> finishedReaders = new LinkedHashSet<Integer>();
	private Method chainableMethod;
	private int chainableMethodIndex;


	@Override
	public void registerInputOutput() {

		// expects the user to call initReader and/or initWriter
		setup();

		//initialize data structures
		for (int i = 0; i < readers.size(); i++) {
			methods.add(null);
			mappings.add(null);
			finishMethods.add(null);
			finishMappings.add(null);
		}

		Method[] methods = this.getClass().getMethods();
		for (Method method : methods) {
			ReadFromWriteTo readFromAnnotation = method.getAnnotation(ReadFromWriteTo.class);
			LastRecordReadFromWriteTo lastReadFromAnnotation = method.getAnnotation(LastRecordReadFromWriteTo.class);


			if (readFromAnnotation != null) {
				int readerIndex = readFromAnnotation.readerIndex();
				int[] writerIndices = readFromAnnotation.writerIndices();

				checkIndices(readerIndex, writerIndices);

				this.mappings.set(readerIndex, writerIndices);
				this.methods.set(readerIndex, method);

				// check method parameters
				Class<?>[] parameterTypes = method.getParameterTypes();
				if (parameterTypes.length != writerIndices.length + 1) {
					throw new IllegalConfigurationException("Method takes wrong number of parameters.");
				}
				if (parameterTypes[0] != readerRecordTypes.get(readerIndex)) {
					throw new IllegalConfigurationException("Method takes wrong input type.");
				}
				for (int i = 1; i < parameterTypes.length; i++) {
					if (parameterTypes[i] != collectors.get(writerIndices[i - 1]).getClass()) {
						throw new IllegalConfigurationException("Method declares wrong output collector type.");
					}
				}
			}


			if (lastReadFromAnnotation != null) {
				int readerIndex = lastReadFromAnnotation.readerIndex();
				int[] writerIndices = lastReadFromAnnotation.writerIndices();

				checkIndices(readerIndex, writerIndices);

				this.finishMappings.set(readerIndex, writerIndices);
				this.finishMethods.set(readerIndex, method);

				// check method parameters
				Class<?>[] parameterTypes = method.getParameterTypes();
				if (parameterTypes.length != writerIndices.length) {
					throw new IllegalConfigurationException("Method takes wrong number of parameters.");
				}
				for (int i = 0; i < parameterTypes.length; i++) {
					if (parameterTypes[i] != collectors.get(writerIndices[i]).getClass()) {
						throw new IllegalConfigurationException("Method declares wrong output collector type.");
					}
				}
			}

		}

		// validation (throws RuntimeException)
		validate();


	}

	private void validate() {

		if (readers.size() == 0) {
			throw new IllegalConfigurationException("Task have to define at least one reader.");
		}

		if (methods.contains(null)) {
			throw new IllegalConfigurationException("Method needs to be implemented for each reader.");
		} // else implies !mappings.contains(null)

		// conditions for a chainable task
		if (readers.size() == 1 && mappings.get(0).length == 1) {
			chainableMethodIndex = 0;
			chainableMethod = methods.get(0);
		}
	}

	private void checkIndices(int readerIndex, int[] writerIndices) {
		if (readerIndex >= readers.size()) {
			throw new IllegalConfigurationException(String.format("Cannot find reader with index %d.", readerIndex));
		}
		for (int writerIndex : writerIndices) {
			if (writerIndex >= collectors.size()) {
				throw new IllegalConfigurationException(String.format("Cannot find writer with index %d.", writerIndex));
			}
		}
	}

	/**
	 * This method is called before the Task registers its readers and writers.
	 * Use this to initialize the readers and writers with initReader and initWriter respectively.
	 */
	protected void setup() {
	}


	/**
	 * This method is called all the readers in this Task are done.
	 * Use this to clean up.
	 */
	protected void shutdown() {
	}

	/**
	 * Initializes a RecordReader associated with an index.
	 *
	 * @param index      the index associated with the reader.
	 * @param recordType the class of records that can be read from the record reader.
	 */
	protected <T extends Record> void initReader(final int index, Class<T> recordType) {
		initReader(index, recordType, new RecordReader<T>(this, recordType));
	}

	/**
	 * Initializes a RecordReader associated with an index.
	 *
	 * @param index      the index associated with the reader.
	 * @param recordType the class of records that can be read from the record reader.
	 * @param reader     the record reader.
	 */
	protected <T extends Record> void initReader(final int index, Class<T> recordType, RecordReader<T> reader) {
		if (index != readers.size()) {
			throw new IllegalConfigurationException("You have to initialize the readers with the indices in order.");
		}
		readers.add(index, reader);
		readerRecordTypes.add(recordType);
		reader.getInputGate().registerRecordAvailabilityListener(new RecordAvailabilityListener<T>() {
			@Override
			public void reportRecordAvailability(InputGate<T> inputGate) {
				notifyReaderAvailability(index);
			}
		});

	}

	/**
	 * Initializes a RecordWriter associated with an index.
	 *
	 * @param index      the index associated with the writer.
	 * @param recordType the class of records that can be emitted with this record writer.
	 */
	protected <T extends Record> void initWriter(int index, Class<T> recordType) {
		initWriter(index, recordType, new RecordWriter<T>(this, recordType));
	}

	/**
	 * Initializes a RecordWriter with a custom ChannelSelector and associated with an index.
	 *
	 * @param index           the index associated with the writer.
	 * @param recordType      the class of records that can be emitted with this record writer.
	 * @param channelSelector the channel selector.
	 */
	protected <T extends Record> void initWriter(int index, Class<T> recordType, ChannelSelector<T> channelSelector) {
		initWriter(index, recordType, new RecordWriter<T>(this, recordType, channelSelector));
	}

	/**
	 * Initializes a RecordWriter with a custom ChannelSelector and associated with an index.
	 *
	 * @param index      the index associated with the writer.
	 * @param recordType the class of records that can be emitted with this record writer.
	 * @param writer     the record writer.
	 */
	protected <T extends Record> void initWriter(int index, Class<T> recordType, RecordWriter<T> writer) {
		if (index != collectors.size()) {
			throw new IllegalConfigurationException("You have to initialize the writers with the indices in order.");
		}
		collectors.add(new Collector<T>(writer));
	}

	/**
	 * Return the RecordReader associated with the given index.
	 *
	 * @param index the index.
	 * @return the record reader.
	 */
	protected RecordReader<? extends Record> getReader(int index) {
		return readers.get(index);
	}

	/**
	 * Return the Collector associated with the given index.
	 *
	 * @param index the index.
	 * @return the collector.
	 */
	protected Collector<? extends Record> getCollector(int index) {
		return collectors.get(index);
	}


	/**
	 * Checks if the Task has a chainable method.
	 *
	 * @return True if this task has a chainable method, false otherwise.
	 */
	public boolean hasChainableMethod() {
		return chainableMethod != null;
	}

	/**
	 * Invokes a chainable method with the given record and writes hte results to the underlying RecordWriter.
	 *
	 * @param record the input for the chainable method
	 * @param <T>    type of the record
	 */
	public <T extends Record> void invokeChainableMethod(T record)
			throws InvocationTargetException, IllegalAccessException, IOException, InterruptedException {
		Collector<? extends Record> collector = collectors.get(mappings.get(chainableMethodIndex)[0]);
		invokeChainableMethod(record, collector);
	}

	/**
	 * Invokes a chainable method with the given record and writes the result only to the buffer
	 * (and not to the underlying RecordWriter).
	 *
	 * @param record the input for the chainable method
	 * @param buffer the buffer for the result
	 * @param <T>    type of the record
	 */
	public <T extends Record> void invokeChainableMethod(T record, Queue<T> buffer)
			throws InvocationTargetException, IllegalAccessException, IOException, InterruptedException {
		MemoryCollector<T> memoryCollector = new MemoryCollector<T>();
		invokeChainableMethod(record, memoryCollector);
		buffer.addAll(memoryCollector.getBuffer());
	}

	private <T extends Record> void invokeChainableMethod(Record record, Collector<? extends Record> collector)
			throws InvocationTargetException, IllegalAccessException, IOException, InterruptedException {
		if (!hasChainableMethod()) {
			throw new IllegalStateException("Task has no chainable method.");
		}
		chainableMethod.invoke(this, record, collector);
		collector.flushBuffer();
	}

	private void notifyEndOfStream(int readerIndex) throws IOException, InterruptedException, InvocationTargetException, IllegalAccessException {
		finishedReaders.add(readerIndex);
		Method method = finishMethods.get(readerIndex);
		if (method != null) {
			method.invoke(this, getCollectors(readerIndex));
			for (int writerIndex : finishMappings.get(readerIndex)) {
				collectors.get(writerIndex).flushBuffer();
			}
		}
	}

	private void notifyReaderAvailability(int index) {
		synchronized (availableReaders) {
			availableReaders.add(index);
			availableReaders.notify();
		}
	}

	private int getAvailableReaderIndex() throws InterruptedException {
		synchronized (availableReaders) {
			while (availableReaders.isEmpty()) {
				availableReaders.wait();
			}
			Iterator<Integer> iterator = availableReaders.iterator();
			int result = -1;
			while (iterator.hasNext()) {
				result = iterator.next();
				iterator.remove();
				if (!finishedReaders.contains(result)) {
					break;
				}
			}

			return result;
		}
	}

	private Object[] getCollectors(int readerIndex) throws IOException, InterruptedException {
		int[] writerIndices = finishMappings.get(readerIndex);
		Object[] args = new Object[writerIndices.length];
		int i = 0;
		for (int writerIndex : writerIndices) {
			args[i++] = collectors.get(writerIndex);
		}
		return args;
	}

	private Object[] getArguments(int readerIndex) throws IOException, InterruptedException {
		RecordReader<? extends Record> reader = readers.get(readerIndex);
		int[] writerIndices = mappings.get(readerIndex);
		Object[] args = new Object[writerIndices.length + 1];
		int i = 0;
		args[i++] = reader.next();
		for (int writerIndex : writerIndices) {
			args[i++] = collectors.get(writerIndex);
		}
		return args;
	}

	private void invokeMethod(int readerIndex) throws IOException, InterruptedException, InvocationTargetException, IllegalAccessException {
		Object[] args = getArguments(readerIndex);
		methods.get(readerIndex).invoke(this, args);
		for (int writerIndex : mappings.get(readerIndex)) {
			collectors.get(writerIndex).flushBuffer();
		}
	}

	@Override
	public void invoke() throws Exception {
		while (finishedReaders.size() < readers.size()) {

			int readerIndex = getAvailableReaderIndex();

			if (readerIndex == -1) {
				continue;
			}

			RecordReader<? extends Record> reader = readers.get(readerIndex);
			switch (reader.hasNextNonBlocking()) {
				case RECORD_AVAILABLE:
					invokeMethod(readerIndex);
					notifyReaderAvailability(readerIndex);
					break;
				case END_OF_STREAM:
					notifyEndOfStream(readerIndex);
					break;
				default:
					break;
			}

		}

		shutdown();

	}
}
