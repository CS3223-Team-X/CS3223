package qp.operators;

import qp.utils.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
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

    private String finalSortedRun;
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
                // some pages could be empty, so we ignore those
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
            finalSortedRun = sortedRuns.get(0);
            sortedRecordsInputStream = new ObjectInputStream(new FileInputStream(finalSortedRun));
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

        try {
            Files.deleteIfExists(Paths.get(UNSORTED_FILE));
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

    /**
     * Merges the specified sorted runs to produce new sorted runs of
     * greater length.
     *
     * @param sortedRuns The sorted runs to merge
     * @return New sorted runs of greater length
     */
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

        deletePreviousSortedRuns(sortedRuns);

        return newSortedRuns;
    }

    /**
     * Opens input streams to the specified sorted runs starting from
     * the start index to the end index exclusive.
     *
     * @param sortedRuns The sorted runs
     * @param startIndex The start index
     * @param endIndex The end index
     * @return Input streams to the sorted runs specified by the indices
     */
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

    /**
     * Closes input streams of sorted runs.
     *
     * @param inputStreams The input stream
     */
    private void closeConnections(List<ObjectInputStream> inputStreams) {
        for (ObjectInputStream inputStream : inputStreams) {
            try {
                inputStream.close();
            } catch (IOException e) {
                // should not reach here
            }
        }
    }

    private void deletePreviousSortedRuns(List<String> previousSortedRuns) {
        for (String previousSortedRun : previousSortedRuns) {
            try {
                Files.deleteIfExists(Paths.get(previousSortedRun));
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Unable to delete a previous sorted run");
            }
        }
    }

    /**
     * Initialise the input pages with one page from each input stream.
     *
     * @param inStreams The input streams from which to
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

    /**
     * Gets the first record for comparison with the subsequent
     * records.
     *
     * @return The first record if exists, else null
     */
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
            // do not read any more
            isEndOfStream = true;
            return null;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean close() {
        try {
            sortedRecordsInputStream.close();
            Files.deleteIfExists(Paths.get(finalSortedRun));
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
