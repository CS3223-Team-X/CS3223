package qp.operators;

import qp.utils.Batch;
import qp.utils.Tuple;

import java.util.ArrayList;
import java.util.List;

public class Buffer {
    private final int numPages;
    private final List<Batch> pages;
    private final List<Integer> incrementalPageSizes;

    public Buffer(int numPages) {
        this.numPages = numPages;
        pages = new ArrayList<>(this.numPages);
        incrementalPageSizes = new ArrayList<>();
    }

    public void addPage(Batch page) {
        pages.add(page);
        computeIncrementalPageSize();
    }

    private void computeIncrementalPageSize() {
        if (pages.size() == 1) {
            incrementalPageSizes.add(pages.get(0).size());
        } else {
            int lastIndex = pages.size() - 1;
            incrementalPageSizes.add(incrementalPageSizes.get(lastIndex - 1) + pages.get(lastIndex).size());
        }
    }

    public Tuple getRecord(int index) {
        for (int i = 0; i < incrementalPageSizes.size(); i++) {
            if (index < incrementalPageSizes.get(i)) {
                if (i == 0) {
                    return pages.get(i).getRecord(index);
                } else {
                    int absoluteIndex = index - incrementalPageSizes.get(i - 1);
                    return pages.get(i).getRecord(absoluteIndex);
                }
            }
        }
        return null;
    }

    public int size() {
        if (pages.isEmpty()) {
            return 0;
        }
        return incrementalPageSizes.get(pages.size() - 1);
    }

    public boolean isEmpty() {
        return pages.isEmpty();
    }

    public boolean hasCapacity() {
        return numPages > pages.size();
    }
}
