package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents the Page Nested Join algorithm.
 */
public class NestedJoin extends Join {
    private static int uniqueFileNumber = 0;         // To get unique filenum for this operation
    private int batchSize;                  // Number of tuples per out batch

    private List<Integer> leftIndices;   // Indices of the join attributes in left table
    private List<Integer> rightIndices;  // Indices of the join attributes in right table

    private String rfname;                  // The file name where the right table is materialized
    private Batch outputBuffer;                 // Buffer page for output
    private Batch leftInputBuffer;                // Buffer page for left input stream
    private Batch rightInputBuffer;               // Buffer page for right input stream
    private ObjectInputStream in;           // File pointer to the right hand materialized file

    int leftCursor;                      // Cursor for left side buffer
    int rightCursor;                      // Cursor for right side buffer
    boolean isLeftEndOfStream;                   // Whether end of stream (left table) is reached
    boolean isEndOfStreamForRight;                   // Whether end of stream (right table) is reached

    public NestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getJoinConditions(), jn.getOpType());
        schema = jn.getSchema();
        joinType = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    public boolean open() {
        /** select number of tuples per batch **/
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        /** find indices attributes of join conditions **/
        leftIndices = new ArrayList<>();
        rightIndices = new ArrayList<>();
        for (Condition con : joinConditions) {
            Attribute leftAttribute = con.getLhs();
            Attribute rightAttribute = (Attribute) con.getRhs();
            leftIndices.add(left.getSchema().indexOf(leftAttribute));
            rightIndices.add(right.getSchema().indexOf(rightAttribute));
        }
        Batch rightpage;

        /** initialize the cursors of input buffers **/
        leftCursor = CURSOR_START;
        rightCursor = CURSOR_START;
        isLeftEndOfStream = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        isEndOfStreamForRight = true;

        /** Right hand side table is to be materialized
         ** for the Nested join to perform
         **/
        if (!right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            uniqueFileNumber++;
            rfname = "NJtemp-" + uniqueFileNumber;
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("NestedJoin: Error writing to temporary file");
                return false;
            }
            if (!right.close())
                return false;
        }
        return left.open();
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {
        int i, j;
        if (isLeftEndOfStream) {
            return null;
        }
        outputBuffer = new Batch(batchSize);
        while (!outputBuffer.isFull()) {
            if (leftCursor == CURSOR_START && isEndOfStreamForRight) {
                /** new left page is to be fetched**/
                leftInputBuffer = left.next();
                if (leftInputBuffer == null) {
                    isLeftEndOfStream = true;
                    return outputBuffer;
                }
                /** Whenever a new left page came, we have to start the
                 ** scanning of right table
                 **/
                try {
                    in = new ObjectInputStream(new FileInputStream(rfname));
                    isEndOfStreamForRight = false;
                } catch (IOException io) {
                    System.err.println("NestedJoin:error in reading the file");
                    System.exit(1);
                }

            }
            while (!isEndOfStreamForRight) {
                try {
                    if (rightCursor == 0 && leftCursor == 0) {
                        rightInputBuffer = (Batch) in.readObject();
                    }
                    for (i = leftCursor; i < leftInputBuffer.size(); ++i) {
                        for (j = rightCursor; j < rightInputBuffer.size(); ++j) {
                            Tuple leftTuple = leftInputBuffer.getRecord(i);
                            Tuple rightTuple = rightInputBuffer.getRecord(j);
                            if (leftTuple.checkJoin(rightTuple, leftIndices, rightIndices)) {
                                Tuple outTuple = leftTuple.joinWith(rightTuple);
                                outputBuffer.addRecord(outTuple);
                                if (outputBuffer.isFull()) {
                                    // both left and right input buffers have been read completely
                                    if (i == leftInputBuffer.size() - 1 && j == rightInputBuffer.size() - 1) {  //case 1
                                        leftCursor = 0;
                                        rightCursor = 0;
                                    } else if (i != leftInputBuffer.size() - 1 && j == rightInputBuffer.size() - 1) {  //case 2
                                        leftCursor = i + 1;
                                        rightCursor = 0;
                                    } else if (i == leftInputBuffer.size() - 1 && j != rightInputBuffer.size() - 1) {  //case 3
                                        leftCursor = i;
                                        rightCursor = j + 1;
                                    } else {
                                        leftCursor = i;
                                        rightCursor = j + 1;
                                    }
                                    return outputBuffer;
                                }
                            }
                        }
                        rightCursor = 0;
                    }
                    leftCursor = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("NestedJoin: Error in reading temporary file");
                    }
                    isEndOfStreamForRight = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("NestedJoin: Error in deserialising temporary file ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("NestedJoin: Error in reading temporary file");
                    System.exit(1);
                }
            }
        }
        return outputBuffer;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        File f = new File(rfname);
        f.delete();
        return true;
    }
}
