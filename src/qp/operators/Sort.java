package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The External Sort algorithm.
 */
public class Sort extends Operator {
    private static int COUNT = 0;
    private static int RECORD_COUNT = 0;
    private static int uniqueFileNumber = 0;
    private static final String FILE_PREFIX = "ext_sort";
    private static final String UNSORTED_FILE = "ext-sort_unsorted";

    private final Operator base;
    private final Direction sortDirection;
    private final List<Attribute> sortAttributes;
    private final List<Integer> sortIndices;
    private final Comparator<Tuple> recordComparator;
    private final int numPages;
    private List<Batch> inputPages;

    private int i = 0;

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

        System.out.println("SORT: " + sortAttributes.get(0).getTabName());

        Batch page;
        int i = 0;
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(UNSORTED_FILE))) {

            while ((page = base.next()) != null) {
                //TODO for some reasons, some pages at the end are empty
                if (page.isEmpty()) {
                    continue;
                }
                i++;
                out.writeObject(page);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        int round = 1;
        List<String> sortedRuns = generateSortedRuns();
        while (sortedRuns.size() > 1) {
            System.out.println("SORT: ROUND: " + round++);
            System.out.println("SORT: SORTED RUNS: " + String.join(", ", sortedRuns));
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
                    // j = 10
                    for (int j = 0; j < inputPages.size(); j++) {
                        Batch page = inputPages.get(j);
                        for (int i = 0; i < page.size(); i++) {
                            Tuple record = records.get(tupleIndex++);
                            page.setRecord(record, i);
                            // some pages might not be full
                            // we fill up the first few pages full with records
                            // then we throw away the other pages
                            if (tupleIndex >= records.size()) {
                                // input pages [x, x, x] [x, x] [x] [x]
                                // {x, x, x, x, x, x, x}
                                // [x, x, x] [x, x, x] [x] []

                                // [] [] []
                                //
                                // say j = 2
                                // sublist from index 0 to 2 inclusive
                                // tuple 100B
                                // page 300 => 3 records
                                // page 400 => 4 records
                                inputPages = inputPages.subList(0, j + 1);
                                // list from 0 to 10 inclusive of length 11
                                break;
                            }
                        }
                    }
                }

                String sortedRun = FILE_PREFIX + uniqueFileNumber++;
                try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(sortedRun))) {
                    System.out.println("SORT: FILE: " + sortedRun);
                    for (Batch page : inputPages) {
                        for (int i = 0; i < page.size(); i++) {
                            System.out.println("SORT: CREATE SORTED RUNS: " + page.getRecord(i).getData(sortIndices.get(0)));
                        }
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
        int i = 0;
        inputPages = new ArrayList<>();
        try {
            while (inputPages.size() < numPages && (page = (Batch) in.readObject()) != null) {
                //TODO stopgap measure; not sure why page can be empty
                if (page.size() != 0) {
                    i++;
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
        // given F files

        // merge them using a buffer of size B - 1; one page for one file
        // whenever the output buffer is full, write it out
        // continue until all pages from B - 1 files are read completely
        // do the same for the remaining F - (B - 1) files iteratively
        // exit this function
        int numPagesForMerging = numPages - 1;
        int startIndex = 0;
        int endIndex = Math.min(sortedRuns.size(), numPagesForMerging);
        int x = 0;
        List<String> run = new ArrayList<>();
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
                System.out.println("SORT: X: " + x++);
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

                    // let x be a page
                    // let [] be a run
                    // B = 3
                    // Sorted runs: [x, x, x], [x, x, x], [x, x, x], [x]


                    // Merge sorted runs
                    // [x, x, x], [x, x, x] => [x, x, x, x, x, x] => 6
                    // [x, x, x], [x]       => [x, x, x, x]       => 4

                    // [x, x, x, x, x, x], [x, x, x, x] => [x, x, x, x, x, x, x, x, x, x] => 10
                    //

                    Tuple rec = inputPages.get(targetIndex).removeAndGetFirst();
//                    System.out.println("SORT: " + rec.getData(sortIndices.get(0)));
                    outputPage.addRecord(rec);
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
                        for (int i = 0; i < outputPage.size(); i++) {
                            run.add((String) outputPage.getRecord(i).getData(sortIndices.get(0)));
//                            System.out.println("SORT: " + outputPage.getRecord(i).getData(sortIndices.get(0)));
                        }
                        System.out.println("SORT: Write to: " + newSortedRun);
//                        System.out.println();
                        COUNT++;
                        out.writeObject(outputPage);

                        // outputPage.clearRecords();
                        outputPage = new Batch(Batch.getPageSize() / schema.getTupleSize());

                    }
                }

                List<String> stringRun = run.stream().map(String::valueOf).collect(Collectors.toUnmodifiableList());
                System.out.println("SORT: " + String.join(", ", stringRun));
                run.clear();

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

//        List<String> newSortedRuns = new ArrayList<>();
//        do {
//
//            Batch outputBuffer = new Batch(Batch.getPageSize() / schema.getTupleSize());
//            String sortedRun = FILE_PREFIX + uniqueFileNumber++;
//            try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(sortedRun))) {
//                while (true) {
//                    Object[] nextRecordToAdd = getFirstRecord();
//                    if (nextRecordToAdd == null) {
//                        break;
//                    }
//                    Tuple targetRecord = (Tuple) nextRecordToAdd[0];
//                    int targetIndex = (int) (nextRecordToAdd[1]);
//                    for (int i = 0; i < inputPages.size(); i++) {
//                        Batch page = inputPages.get(i);
//                        Tuple record = page.getNextRecord();
//                        if (record == null) {
//                            continue;
//                        }
//                        if (record != targetRecord) {
//                            int result = Tuple.compare(targetRecord, record, this.sortIndices);
//                            if (result < 0) {
//                                switch (sortDirection) {
//                                    case ASC:
//                                        continue;
//                                    case DSC:
//                                        targetIndex = i;
//                                        targetRecord = record;
//                                        break;
//                                    default:
//                                        throw new RuntimeException();
//                                }
//                            }
//                            if (result > 0) {
//                                switch (sortDirection) {
//                                    case ASC:
//                                        targetIndex = i;
//                                        targetRecord = record;
//                                        break;
//                                    case DSC:
//                                        continue;
//                                    default:
//                                        throw new RuntimeException();
//                                }
//                            }
//                        }
//                    }
//
//                    outputBuffer.addRecord(inputPages.get(targetIndex).removeAndGetFirst());
//                    if (inputPages.get(targetIndex).isEmpty()) {
//                        try {
//                            Batch nextPage = (Batch) inputStreams.get(targetIndex).readObject();
//                            if (nextPage != null) {
//                                inputPages.set(targetIndex, nextPage);
//                            }
//                        } catch (EOFException e) {
//                            // nothing left to read
//
//                        } catch (ClassNotFoundException e) {
//                            e.printStackTrace();
//                        }
//                    }
//                    if (outputBuffer.isFull()) {
//                        out.writeObject(outputBuffer);
//                        outputBuffer.clearRecords();
//                    }
//                }
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//
//            newSortedRuns.add(sortedRun);
//            readIntoInputBuffer(inputStreams.toArray(new ObjectInputStream[0]));
//        } while (!inputPages.isEmpty());
//        return newSortedRuns;
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
            i++;
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
