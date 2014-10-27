package eu.stratosphere.nephele.streaming.taskmanager.qosmanager.buffers;

/**
 * Stores a time-series of generic values. Instances of this class are usually
 * associated by the Qos data of a Qos edge.
 * 
 * @author Bjoern Lohrmann
 * 
 */
public class ValueHistory<T> {

	private Object[] entries;

	private int entriesInHistory;

	public ValueHistory(int noOfHistoryEntries) {
		this.entries = new Object[noOfHistoryEntries];
		this.entriesInHistory = 0;
	}

	public void addToHistory(long timestamp, T newValue) {
		HistoryEntry<T> newEntry = new HistoryEntry<T>(Math.min(
				this.entriesInHistory, this.entries.length - 1), timestamp,
				newValue);

		if (this.entriesInHistory < this.entries.length) {
			this.entries[this.entriesInHistory] = newEntry;
			this.entriesInHistory++;
		} else {
			System.arraycopy(this.entries, 1, this.entries, 0,
					this.entriesInHistory - 1);
			this.entries[this.entriesInHistory - 1] = newEntry;
		}
	}

	@SuppressWarnings("unchecked")
	public HistoryEntry<T> getFirstEntry() {
		return (HistoryEntry<T> ) this.entries[0];
	}

	@SuppressWarnings("unchecked")
	public HistoryEntry<T> getLastEntry() {
		if (this.entriesInHistory > 0) {
			return (HistoryEntry<T>) this.entries[this.entriesInHistory - 1];
		}

		return null;
	}

	/** Returns all entries greater or equal a given timestamp. */
	@SuppressWarnings("unchecked")
	public HistoryEntry<T>[] getLastEntries(long minTimestamp) {
		if (this.entriesInHistory > 0) {
			int startIndex;

			for (startIndex = this.entriesInHistory; startIndex > 0; startIndex--) {
				if (((HistoryEntry<T>) this.entries[startIndex - 1]).getTimestamp() < minTimestamp) {
					break;
				}
			}

			if (startIndex < this.entriesInHistory) {
				HistoryEntry<T>[] result = new HistoryEntry[this.entriesInHistory - startIndex];
				System.arraycopy(entries, startIndex, result, 0, this.entriesInHistory - startIndex);
				return result;

			} else {
				return new HistoryEntry[0];
			}

		} else {
			return new HistoryEntry[0];
		}
	}

	@SuppressWarnings("unchecked")
	public HistoryEntry<T>[] getEntries() {
		if (hasEntries()) {
			HistoryEntry<T> result[] = new HistoryEntry[this.entriesInHistory];
			System.arraycopy(this.entries, 0, result, 0, this.entriesInHistory);
			return result;
		} else {
			return new HistoryEntry[0];
		}
	}

	public boolean hasEntries() {
		return this.entriesInHistory > 0;
	}

	public int getNumberOfEntries() {
		return this.entriesInHistory;
	}

	public int getMaxNumberOfEntries() {
		return this.entries.length;
	}
}
