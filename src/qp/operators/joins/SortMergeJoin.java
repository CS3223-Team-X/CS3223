package qp.operators.joins;
import qp.utils.Batch;
import qp.operators.Sort;
import qp.utils.Attribute;
import qp.utils.Condition;
import qp.utils.Tuple;
import qp.operators.Buffer;

import java.io.*;
import java.nio.file.Files;
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

    // the index of first right tuple we found a matching in current iteration 
    int currFirstMatchingTupleIndex;

    // the absolute index of the first batch in current right buffer
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

    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
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

        // Get sorted tables using external sort algorithm for both left and right tables.
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
        
        // Materialize each page in sorted right table into a file to faciliate possible backtrackings.
        try {
            while ((rightpage = sortedRight.next()) != null) {
                String rfname = "SMJtemp-" + fileIndex;
                File file = new File(rfname);
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(file));
                out.writeObject(rightpage);
                out.close();
                rightPages.add(file);
                // System.out.println("SortMergeJoin:" + fileIndex);
                fileIndex++;
            }
        } catch (IOException io) {
            // System.out.println("SortMergeJoin: Error writing to temporary file");
            return false;
        }

        // allocate B - 2 buffers for right input, 1 for output, 1 for left input.
        rightBufferSize = getNumBuff() - 2;
        rightBatchIndexOffset = 0;
        rightTupleIndex = 0;
        
        try {
            createRightBuffer(rightBufferSize, 0);
        } catch (IndexOutOfBoundsException e) {
            // System.out.println("SortMergeJoin: Right pages are all read in");
        }

        return true;
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    @Override
    public Batch next() {
        if (isLeftEndOfStream || isRightEndOfStream) {
            // System.out.println("SortMergeJoin: return null");
            return null;
        }

        outputBuffer = new Batch(batchSize);

        while(!outputBuffer.isFull()) {
            if (isLeftEndOfStream || isRightEndOfStream) {
                break;
            }

            // Fetch a new page to left input buffer from sorted left table if we have already gone through original right input buffer.
            if (leftCursor == CURSOR_START && retriveNewLeftPage) {
                //System.out.println("SortMergeJoin: retrive new left page");
                leftInputBatch = sortedLeft.next();
                retriveNewLeftPage = false;

                // If sorted left table runs out
                if (leftInputBatch == null || leftInputBatch.isEmpty()) {
                    // System.out.println("SortMergeJoin: sorted left table run out");
                    isLeftEndOfStream = true;
                    break;
                }
            }

            Tuple leftTuple;
            Tuple rightTuple;

            //System.out.println("SortMergeJoin: right tuple index: " + rightTupleIndex);

            // Get the next tuple from each table
            leftTuple = leftInputBatch.getRecord(leftCursor);
            if (leftTuple == null) {
                isLeftEndOfStream = true;
                // System.out.println("SortMergeJoin: left table run out");
                break;
            }

            rightTuple = getRightTuple(rightTupleIndex);
            if (rightTuple == null) {
                isRightEndOfStream = true;

                // if sorted right table runs out but the previous tuples matches - backtracking
                if (isPrevTuplesMatch) {
                    //System.out.println("SortMergeJoin: go to next left tuple");
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
                // System.out.println("SortMergeJoin: right table run out");
                continue;
            }

            int compareRes = Tuple.compareTuples(leftTuple, rightTuple, leftIndices.get(0), rightIndices.get(0));

            // advance left cursor
            if (compareRes < 0) {
                //System.out.println("SortMergeJoin: advance left cursor");
                //System.out.println(leftTuple.getData().get(leftIndices.get(0)) + " " + rightTuple.getData().get(rightIndices.get(0)));
                if (leftCursor == leftInputBatch.size() - 1) {
                    // retrieve the next page of left table
                    leftCursor = CURSOR_START;
                    retriveNewLeftPage = true;
                } else {
                    leftCursor++;
                }

                // backtrack the right table
                if (isPrevTuplesMatch) {
                    rightTupleIndex = currFirstMatchingTupleIndex;
                    //System.out.println("SortMergeJoin: go to next left tuple");
                }

                // reset the boolean flag
                isPrevTuplesMatch = false;
            } else if (compareRes > 0) {
                // advance right cursor
                        //System.out.println("SortMergeJoin: advance right cursor");
                        //System.out.println(leftTuple.getData().get(leftIndices.get(0)) + " " + rightTuple.getData().get(rightIndices.get(0)));
                rightTupleIndex++;
                
                // reset the boolean flag
                isPrevTuplesMatch = false;
            } else {
                // found matching tuples and add it to the output buffer
                        //System.out.println("SortMergeJoin: found matching");
                        //System.out.println(leftTuple.getData().get(leftIndices.get(0)) + " " + rightTuple.getData().get(rightIndices.get(0)));
                Tuple outTuple = leftTuple.joinWith(rightTuple);
                outputBuffer.addRecord(outTuple);

                if (!isPrevTuplesMatch) {
                    currFirstMatchingTupleIndex = rightTupleIndex;
                }
                isPrevTuplesMatch = true;
                rightTupleIndex++;
            }         
        }
        // System.out.println("SortMergeJoin: write out");
        return outputBuffer;
    }

    /**
     * Get the tuple from right table with the specified index
     */
    private Tuple getRightTuple(int rightTupleIndex) {
        // Calculate the size of each right batch
        int rightTupleSize = getRight().getSchema().getTupleSize();
        int rightBatchSize = Batch.getPageSize() / rightTupleSize;

        // The index of the batch where the tuple we want exists
        int batchIndex = rightTupleIndex / rightBatchSize;
        int offset = rightTupleIndex % rightBatchSize; // offset of the tuple within the batch it belongs to

        // the index of the batch inside the current buffer
        int absoluteBatchIndex = batchIndex - rightBatchIndexOffset;

        if (absoluteBatchIndex >= 0 && absoluteBatchIndex < rightBufferSize) {
            // the batch we want is already inside buffer
            int absoluteTupleIndex = absoluteBatchIndex * rightBatchSize + offset;
            return rightInputBuffer.getRecord(absoluteTupleIndex);
        } else {
            // not inside buffer
            try {
                // update the right buffer by putting in list of batches starting from the batch we want now. (the batch include the tuple we want)
                createRightBuffer(rightBufferSize, batchIndex);
            } catch (IndexOutOfBoundsException e) {
                // System.out.println("SortMergeJoin: Right pages are all read in");
            }
            // System.out.println("SortMergeJoin: retrive new right buffer");
            rightBatchIndexOffset = batchIndex;
            int absoluteTupleIndex = offset;
            // System.out.println("SortMergeJoin: absolute tuple index: " + absoluteTupleIndex);
            return rightInputBuffer.getRecord(absoluteTupleIndex); // fetch the tuple we want from the updated buffer
        }
    }

    /**
     * Initialize the right buffer when we first open the operator 
     * or read in new pages from the right table into buffers when needed.
     */
    private boolean createRightBuffer(int bufferSize, int startIndex) throws IndexOutOfBoundsException {
        // create a new buffer object with buffer size allocated
        rightInputBuffer = new Buffer(bufferSize);

        // the index of the first batch we need to put into current buffer
        int idx = startIndex;

        while (true) {
            Batch batch = null;
            try {
                // read in the batch with the specified index from disk
                ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(rightPages.get(idx)));
                batch = (Batch) objectInputStream.readObject();
                objectInputStream.close();
            } catch (IOException | ClassNotFoundException io) {
                // System.err.println("SortMergeJoin: error in reading the file " + idx);
                io.printStackTrace();
                System.exit(1);
            }
            if (rightInputBuffer.isEmpty() && batch == null) {
                rightInputBuffer = null;
                break;
            }
            if (batch == null) {
                // break if the batch fetched is null
                break;
            }
            if (!rightInputBuffer.hasCapacity()) {
                break;
            }
            // add the batch fetched into the right input buffer
            rightInputBuffer.addPage(batch);
            // System.out.println("SortMergeJoin: add page to right buffer " + idx);
            idx++;
        }
        return true;
    }

    /**
     * Closes this operator and delete files used for materialized right table.
     *
     * @return true if the operator is closed successfully.
     */
    @Override
    public boolean close() {
        left.close();
        right.close();
        sortedLeft.close();
        sortedRight.close();
        for (File file : rightPages) {
            try {
                Files.deleteIfExists(file.toPath());
            } catch (IOException e) {
                continue;
            }
        }
        return super.close();
    }
}
