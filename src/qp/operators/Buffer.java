package qp.operators;

import qp.utils.Batch;
import qp.utils.Tuple;

import java.util.ArrayList;
import java.util.List;

public class Buffer {
    private final int numPages;
    private final List<Batch> pages;
    private final int[] incrementalSizes;

    public Buffer(int numPages) {
        this.numPages = numPages;
        pages = new ArrayList<>(this.numPages);
        incrementalSizes = computeIncrementalSizes();
    }

    private int[] computeIncrementalSizes() {
        int[] incrementalPageSizes = new int[numPages];
        incrementalPageSizes[0] = pages.get(1).size();
        for (int i = 1; i < numPages; i++) {
            incrementalPageSizes[i] = incrementalPageSizes[i - 1] + pages.get(i).size();
        }
        return incrementalPageSizes;
    }

    public void addPage(Batch page) {
        pages.add(page);
    }

    public Tuple getRecord(int index) {
        for (int i = 0; i < incrementalSizes.length; i++) {
            if (index < incrementalSizes[i]) {
                int absoluteIndex = index - i * Batch.getPageSize();
                return pages.get(i).getRecord(absoluteIndex);
            }
        }
        return null;
    }

    public int size() {
        return incrementalSizes[numPages - 1];
    }

    public boolean isEmpty() {
        return pages.isEmpty();
    }
}
