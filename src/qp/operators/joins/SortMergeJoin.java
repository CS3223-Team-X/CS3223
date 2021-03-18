package qp.operators.joins;
import qp.utils.Batch;
import qp.operators.Sort;
import qp.utils.Attribute;
import qp.utils.Condition;
import qp.utils.Tuple;
import qp.operators.Buffer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class SortMergeJoin extends Join {
    private int batchSize;                  // Number of tuples per out batch
    
    protected final Join join;

    private List<Integer> leftIndices;   // Indices of the join attributes in left table
    private List<Integer> rightIndices;  // Indices of the join attributes in right table
    private List<Attribute> leftAttrs;
    private List<Attribute> rightAttrs;

    private Batch leftInputBatch;
    private Buffer rightInputBuffer;
    private Batch outputBuffer;

    int leftCursor;                       // Cursor for left side buffer
    boolean isLeftEndOfStream;            // Whether end of stream (left table) is reached
    boolean isRightEndOfStream;           // Whether end of stream (right table) is reached

    // This index is unique for each tuple in a table
    int rightTupleIndex;

    // Materialized right table stored in disk for possible backtracking
    List<File> rightPages;

    // Used for backtracking
    int currFirstMatchingTupleIndex;

    int rightBatchIndexOffset;
    
    boolean isPrevTuplesMatch = false;

    boolean retriveNewLeftPage;

    Sort sortedLeft;
    Sort sortedRight;

    private int rightBufferSize;

    public SortMergeJoin(Join join) {
        super(join.getLeft(), join.getRight(), join.getJoinConditions());
        this.join = join;
        schema = join.getSchema();
        joinType = join.getJoinType();
        numBuff = join.getNumBuff();
    }

    @Override
    public boolean open() {
        /** select number of tuples per batch **/
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;
        
        /** find indices attributes of join conditions **/
        leftIndices = new ArrayList<>();
        rightIndices = new ArrayList<>();
        leftAttrs = new ArrayList<>();
        rightAttrs = new ArrayList<>();

        for (Condition joinCondition : joinConditions) {
            Attribute leftAttribute = joinCondition.getLhs();
            Attribute rightAttribute = (Attribute) joinCondition.getRhs();
            leftAttrs.add(leftAttribute);
            rightAttrs.add(rightAttribute);
            leftIndices.add(left.getSchema().indexOf(leftAttribute));
            rightIndices.add(right.getSchema().indexOf(rightAttribute));
        }

        /** initialize the cursors of input buffers **/
        leftCursor = CURSOR_START;
        retriveNewLeftPage = true;

        if (!left.open() || !right.open()){
            System.out.println("SortMergeJoin: Error opening left or right table");
			return false;
		}

        isLeftEndOfStream = false;
        isRightEndOfStream = false;



        sortedLeft = new Sort(left, leftAttrs, Sort.Direction.ASC, numBuff);
        sortedRight = new Sort(right, rightAttrs, Sort.Direction.ASC, numBuff);

        if (!sortedLeft.open() || !sortedRight.open()){
            System.out.println("SortMergeJoin: Error opening sorted left or sorted right table");
			return false;
		}

        /** If the right operator is not a base table then
         ** Materialize the intermediate result from right
         ** into a file
         **/
        int fileIndex = 0;
        Batch rightpage;
        rightPages = new ArrayList<>();

        try {
            while ((rightpage = sortedRight.next()) != null) {
                String rfname = "SMJtemp-" + fileIndex;
                File file = new File(rfname);
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(file));
                out.writeObject(rightpage);
                // out.close();
                rightPages.add(file);
                System.out.println("SortMergeJoin:" + fileIndex);
                fileIndex++;
            }
        } catch (IOException io) {
            System.out.println("SortMergeJoin: Error writing to temporary file");
            return false;
        }

        rightBufferSize = getNumBuff() - 2;
        rightBatchIndexOffset = 0;
        rightTupleIndex = 0;
        
        try {
            createRightBuffer(rightBufferSize, 0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("SortMergeJoin: Right pages are all read in");
        }

        return true;
    }

    @Override
    public Batch next() {
        if (isLeftEndOfStream || isRightEndOfStream) {
            System.out.println("SortMergeJoin: return null");
            return null;
        }

        outputBuffer = new Batch(batchSize);

        while(!outputBuffer.isFull()) {
            if (isLeftEndOfStream || isRightEndOfStream) {
                break;
            }
            if (leftCursor == CURSOR_START && retriveNewLeftPage) {
                System.out.println("SortMergeJoin: retrive new left page");
                leftInputBatch = sortedLeft.next();
                retriveNewLeftPage = false;
                if (leftInputBatch == null || leftInputBatch.isEmpty()) {
                    System.out.println("SortMergeJoin: sorted left table run out");
                    isLeftEndOfStream = true;
                    break;
                }
            }

            Tuple leftTuple;
            Tuple rightTuple;

            System.out.println("SortMergeJoin: right tuple index: " + rightTupleIndex);

            // Get one tuple from each table
            leftTuple = leftInputBatch.getRecord(leftCursor);
            if (leftTuple == null) {
                isLeftEndOfStream = true;
                System.out.println("SortMergeJoin: left table run out");
                break;
            }

            rightTuple = getRightTuple(rightTupleIndex);
            if (rightTuple == null) {
                isRightEndOfStream = true;
                if (isPrevTuplesMatch) {
                    System.out.println("SortMergeJoin: go to next left tuple");
                    rightTupleIndex = currFirstMatchingTupleIndex;
                    isPrevTuplesMatch = false;
                    isRightEndOfStream = false;
                    if (leftCursor == leftInputBatch.size() - 1) {
                        // retrieve the next page of left table
                        leftCursor = CURSOR_START;
                        retriveNewLeftPage = true;
                    } else {
                        leftCursor++;
                    }    
                }
                System.out.println("SortMergeJoin: right table run out");
                continue;
            }

            int compareRes = Tuple.compareTuples(leftTuple, rightTuple, leftIndices.get(0), rightIndices.get(0));

            if (compareRes < 0) {
                // advance left cursor
                System.out.println("SortMergeJoin: advance left cursor");
                System.out.println(leftTuple.getData().get(leftIndices.get(0)) + " " + rightTuple.getData().get(rightIndices.get(0)));
                if (leftCursor == leftInputBatch.size() - 1) {
                    // retrieve the next page of left table
                    leftCursor = CURSOR_START;
                    retriveNewLeftPage = true;
                } else {
                    leftCursor++;
                }

                if (isPrevTuplesMatch) {
                    rightTupleIndex = currFirstMatchingTupleIndex;
                    System.out.println("SortMergeJoin: go to next left tuple");
                }

                // reset the boolean flag
                isPrevTuplesMatch = false;
            } else if (compareRes > 0) {
                // advance right cursor
                System.out.println("SortMergeJoin: advance right cursor");
                System.out.println(leftTuple.getData().get(leftIndices.get(0)) + " " + rightTuple.getData().get(rightIndices.get(0)));
                rightTupleIndex++;
                
                // reset the boolean flag
                isPrevTuplesMatch = false;
            } else {
                // found matching tuples
                System.out.println("SortMergeJoin: found matching");
                System.out.println(leftTuple.getData().get(leftIndices.get(0)) + " " + rightTuple.getData().get(rightIndices.get(0)));
                Tuple outTuple = leftTuple.joinWith(rightTuple);
                outputBuffer.addRecord(outTuple);
                if (!isPrevTuplesMatch) {
                    currFirstMatchingTupleIndex = rightTupleIndex;
                }
                isPrevTuplesMatch = true;
                rightTupleIndex++;
            }         
        }
        System.out.println("SortMergeJoin: write out");
        return outputBuffer;
    }

    private Tuple getRightTuple(int rightTupleIndex) {
        int rightTupleSize = getRight().getSchema().getTupleSize();
        int rightBatchSize = Batch.getPageSize() / rightTupleSize;

        int batchIndex = rightTupleIndex / rightBatchSize;
        int offset = rightTupleIndex % rightBatchSize;

        int absoluteBatchIndex = batchIndex - rightBatchIndexOffset;

        if (absoluteBatchIndex >= 0 && absoluteBatchIndex < rightBufferSize) {
            // the batch we want is already inside buffer
            int absoluteTupleIndex = absoluteBatchIndex * rightBatchSize + offset;
            return rightInputBuffer.getRecord(absoluteTupleIndex);
        } else {
            // not inside buffer (during backtracking)
            try {
                createRightBuffer(rightBufferSize, batchIndex);
            } catch (IndexOutOfBoundsException e) {
                System.out.println("SortMergeJoin: Right pages are all read in");
            }
            System.out.println("SortMergeJoin: retrive new right buffer");
            rightBatchIndexOffset = batchIndex;
            int absoluteTupleIndex = offset;
            System.out.println("SortMergeJoin: absolute tuple index: " + absoluteTupleIndex);
            return rightInputBuffer.getRecord(absoluteTupleIndex);
        }
    }

    private boolean createRightBuffer(int bufferSize, int startIndex) throws IndexOutOfBoundsException {
        rightInputBuffer = new Buffer(bufferSize);
        int idx = startIndex;
        while (true) {
            Batch batch = null;
            try {
                ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(rightPages.get(idx)));
                batch = (Batch) objectInputStream.readObject();
            } catch (IOException | ClassNotFoundException io) {
                System.err.println("SortMergeJoin: error in reading the file");
                System.exit(1);
            }
            if (rightInputBuffer.isEmpty() && batch == null) {
                rightInputBuffer = null;
                break;
            }
            if (batch == null) {
                break;
            }
            if (!rightInputBuffer.hasCapacity()) {
                break;
            }
            rightInputBuffer.addPage(batch);
            System.out.println("SortMergeJoin: add page to right buffer " + idx);
            idx++;
        }
        return true;
    }

    /**
     * Closes this operator.
     *
     * @return true if the operator is closed successfully.
     */
    @Override
    public boolean close() {
        left.close();
        right.close();
        //sortedLeft.close();
        //sortedRight.close();
        for (File file : rightPages) {
            file.delete();
        }
        return super.close();
    }
}
