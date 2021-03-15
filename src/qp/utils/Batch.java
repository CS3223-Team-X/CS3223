package qp.utils;

import java.util.ArrayList;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

/**
 * Represents a page.
 */
public class Batch implements Serializable {
    /**
     * Number of bytes per page
     */
    private static int pageSize;

    private final int numTuples;

    private final List<Tuple> tuples;

    public static void setPageSize(int size) {
        pageSize = size;
    }

    public static int getPageSize() {
        return pageSize;
    }

    public Batch(int numTuples) {
        this.numTuples = numTuples;
        tuples = new ArrayList<>(this.numTuples);
    }

    public int capacity() {
        return numTuples;
    }

    public boolean isFull() {
        return size() == capacity();
    }

    public Tuple getRecord(int i) {
        return tuples.get(i);
    }

    public void setRecord(Tuple t, int i) {
        tuples.set(i, t);
    }

    public List<Tuple> getRecords() {
        return tuples;
    }

    public Tuple getNextRecord() {
        return tuples.get(0);
    }

    public void addRecord(Tuple t) {
        tuples.add(t);
    }

    public void addRecord(Tuple t, int i) {
        tuples.add(i, t);
    }

    public void removeRecord(int i) {
        tuples.remove(i);
    }

    public boolean hasMore() {
        return !tuples.isEmpty();
    }

    public Tuple removeAndGetFirst() {
        return tuples.remove(0);
    }

    public void clearRecords() {
        tuples.clear();
    }

    public boolean containsRecord(Tuple t) {
        return tuples.contains(t);
    }

    public int indexOfRecord(Tuple t) {
        return tuples.indexOf(t);
    }

    public boolean isEmpty() {
        return tuples.isEmpty();
    }

    public int size() {
        return tuples.size();
    }

    public void sort(Comparator<Tuple> comparator) {
        tuples.sort(comparator);
    }
}