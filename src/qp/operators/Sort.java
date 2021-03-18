package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The External Sort algorithm.
 */
public class Sort extends Operator {
    private static int uniqueFileNumber = 0;
    private static final String FILE_PREFIX = "ext-sort";
    private static final String UNSORTED_FILE = "ext-sort_unsorted";

    private final Operator base;
    private final Direction sortDirection;
    private final List<Attribute> sortAttributes;
    private final List<Integer> sortIndices;
    private final Comparator<Tuple> recordComparator;
    private final int numPages;
    private List<Batch> inputPages;

    private ObjectInputStream sortedRecordsInputStream;
    private boolean isEndOfStream;

    /**
     * Constructs an instance with an allocated buffer size.
     *
     * @param numPages The buffer size
     */
    public Sort(Operator base, List<Attribute> sortAttributes, Sort.Direction sortDirection, int numPages) {
        super(OperatorType.ORDER);
        this.base = base;
        this.schema = base.getSchema();

        this.sortDirection = sortDirection;
        this.sortAttributes = sortAttributes;
        this.sortIndices = computeSortIndices(this.base.getSchema(), this.sortAttributes);
        recordComparator = generateTupleComparator(this.sortDirection, this.sortIndices);

        this.numPages = numPages;
        inputPages = new ArrayList<>(numPages);
    }

    private List<Integer> computeSortIndices(Schema schema, List<Attribute> sortedAttributes) {
        List<Integer> sortIndices = new ArrayList<>();
        for (Attribute sortAttribute : sortedAttributes) {
            int sortIndex = schema.indexOf(sortAttribute);
            sortIndices.add(sortIndex);
        }
        return sortIndices;
    }

    private Comparator<Tuple> generateTupleComparator(Sort.Direction sortDirection, List<Integer> sortIndices) {
        switch (sortDirection) {
            case ASC:
                return (o1, o2) -> Tuple.compare(o1, o2, sortIndices);
            case DSC:
                return (o1, o2) -> Tuple.compare(o2, o1, sortIndices);
            default:
                throw new RuntimeException();
        }
    }

    @Override
    public boolean open() {
        if (!base.open()) {
            return false;
        }

        Batch page;
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(UNSORTED_FILE))) {

            while ((page = base.next()) != null) {
                //TODO for some reasons, some pages at the end are empty
                if (page.isEmpty()) {
                    continue;
                }
                out.writeObject(page);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        List<String> sortedRuns = generateSortedRuns();
        while (sortedRuns.size() > 1) {
            sortedRuns = mergeSortedRuns(sortedRuns);

        }
        try {
            sortedRecordsInputStream = new ObjectInputStream(new FileInputStream(sortedRuns.get(0)));
            isEndOfStream = false;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return base.close();
    }

    /**
     * Generates the initial sorted runs.
     *
     * @return The sorted runs
     */
    private List<String> generateSortedRuns() {
        List<String> sortedRuns = new ArrayList<>();

        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(UNSORTED_FILE))) {
            readIntoInputBuffer(in);

            do {
                List<Tuple> records = new ArrayList<>();
                for (Batch batch : inputPages) {
                    records.addAll(batch.getRecords());
                }
                records.sort(recordComparator);

                int tupleIndex = 0;
                while (tupleIndex < records.size()) {
                    for (int j = 0; j < inputPages.size(); j++) {
                        Batch page = inputPages.get(j);
                        for (int i = 0; i < page.size(); i++) {
                            Tuple record = records.get(tupleIndex++);
                            page.setRecord(record, i);
                            // some pages might not be full
                            // we fill up the first few pages full with records
                            // then we throw away the other pages
                            if (tupleIndex >= records.size()) {
                                inputPages = inputPages.subList(0, j + 1);
                                break;
                            }
                        }
                    }
                }

                String sortedRun = FILE_PREFIX + uniqueFileNumber++;
                try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(sortedRun))) {
                    for (Batch page : inputPages) {
                        out.writeObject(page);
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
                sortedRuns.add(sortedRun);
                readIntoInputBuffer(in);
            } while (!inputPages.isEmpty());

        } catch (IOException e) {
            e.printStackTrace();
        }

        return sortedRuns;
    }

    private void readIntoInputBuffer(ObjectInputStream in) {
        Batch page;
        inputPages = new ArrayList<>();
        try {
            while (inputPages.size() < numPages && (page = (Batch) in.readObject()) != null) {
                //TODO stopgap measure; not sure why page can be empty
                if (page.size() != 0) {
                    inputPages.add(page);
                }
            }
        } catch (EOFException e) {
            // do not read anymore
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private List<String> mergeSortedRuns(List<String> sortedRuns) {
        int numPagesForMerging = numPages - 1;
        int startIndex = 0;
        int endIndex = Math.min(sortedRuns.size(), numPagesForMerging);
        List<String> newSortedRuns = new ArrayList<>();
        do {
            // open connections to B - 1 files
            List<ObjectInputStream> inputStreams = openConnections(sortedRuns, startIndex, endIndex);
            // init input pages with the first page from each file
            initInputPages(inputStreams.toArray(new ObjectInputStream[0]));

            // init output page
            Batch outputPage = new Batch(Batch.getPageSize() / schema.getTupleSize());
            String newSortedRun = FILE_PREFIX + uniqueFileNumber++;
            try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(newSortedRun))) {
                // do B - 1 merge
                while (true) {
                    Object[] nextRecordToAdd = getFirstRecord();
                    if (nextRecordToAdd == null) {
                        // no more records to add
                        break;
                    }
                    Tuple targetRecord = (Tuple) nextRecordToAdd[0];
                    int targetIndex = (int) (nextRecordToAdd[1]);
                    for (int i = 0; i < inputPages.size(); i++) {
                        Batch page = inputPages.get(i);
                        Tuple record = page.getNextRecord();
                        if (record == null) {
                            continue;
                        }
                        if (record != targetRecord) {
                            int result = Tuple.compare(targetRecord, record, this.sortIndices);
                            if (result < 0) {
                                switch (sortDirection) {
                                    case ASC:
                                        continue;
                                    case DSC:
                                        targetIndex = i;
                                        targetRecord = record;
                                        break;
                                    default:
                                        throw new RuntimeException();
                                }
                            }
                            if (result > 0) {
                                switch (sortDirection) {
                                    case ASC:
                                        targetIndex = i;
                                        targetRecord = record;
                                        break;
                                    case DSC:
                                        continue;
                                    default:
                                        throw new RuntimeException();
                                }
                            }
                        }
                    }

                    outputPage.addRecord(inputPages.get(targetIndex).removeAndGetFirst());
                    if (inputPages.get(targetIndex).isEmpty()) {
                        try {
                            Batch nextPage = (Batch) inputStreams.get(targetIndex).readObject();
                            if (nextPage != null) {
                                inputPages.set(targetIndex, nextPage);
                            }
                        } catch (EOFException e) {
                            // nothing left to read

                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    }

                    if (outputPage.isFull() || getFirstRecord() == null) {
                        out.writeObject(outputPage);
                        outputPage = new Batch(Batch.getPageSize() / schema.getTupleSize());
                    }
                }

                // get ready for the next B - 1 files
                startIndex = endIndex;
                if (sortedRuns.size() - endIndex < numPagesForMerging) {
                    // the number of files left to merge is less than B - 1 pages
                    endIndex = sortedRuns.size();
                } else {
                    // the number of files left to merge is more than or equal to B - 1 pages
                    endIndex += numPagesForMerging;
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

            // store the new sorted run
            newSortedRuns.add(newSortedRun);
            closeConnections(inputStreams);
        } while (startIndex < endIndex && endIndex <= sortedRuns.size());

        return newSortedRuns;
    }

    private List<ObjectInputStream> openConnections(List<String> sortedRuns, int startIndex, int endIndex) {
        List<ObjectInputStream> inputStreams = new ArrayList<>(endIndex - startIndex);
        try {
            for (int i = startIndex; i < endIndex; i++) {
                String sortedRun = sortedRuns.get(i);
                inputStreams.add(new ObjectInputStream(new FileInputStream(sortedRun)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return inputStreams;
    }

    private void closeConnections(List<ObjectInputStream> inputStreams) {
        for (ObjectInputStream inputStream : inputStreams) {
            try {
                inputStream.close();
            } catch (IOException e) {
                // should not reach here
            }
        }
    }

    /**
     * Initialise the input pages with one page from each file connection.
     *
     * @param inStreams
     */
    private void initInputPages(ObjectInputStream... inStreams) {
        inputPages = new ArrayList<>();

        for (ObjectInputStream inStream : inStreams) {
            try {
                Batch page = (Batch) inStream.readObject();
                if (page != null) {
                    inputPages.add(page);
                }
            } catch (EOFException e) {
                // nothing left to read
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    private Object[] getFirstRecord() {
        for (int i = 0; i < inputPages.size(); i++) {
            Batch page = inputPages.get(i);
            if (page.hasMore()) {
                return new Object[] {page.getNextRecord(), i};
            }
        }
        return null;
    }

    @Override
    public Batch next() {
        if (isEndOfStream) {
            return null;
        }

        try {
            return (Batch) sortedRecordsInputStream.readObject();
        } catch (EOFException | NullPointerException e) {
            isEndOfStream = true;
            return null;
            // do not read any more
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean close() {
        try {
            sortedRecordsInputStream.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public Object clone() {
        List<Attribute> newSortAttributes = new ArrayList<>();
        for (Attribute attribute : sortAttributes) {
            newSortAttributes.add((Attribute) attribute.clone());
        }
        Sort newSort = new Sort((Operator) base.clone(), newSortAttributes, sortDirection, numPages);
        newSort.setSchema((Schema) base.getSchema().clone());
        return newSort;
    }

    public enum Direction {
        ASC, DSC
    }
}
