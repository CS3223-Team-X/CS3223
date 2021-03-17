package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.List;
import java.util.Vector;

/**
 * This class helps to implement Distinct and remove duplicates from a list.
 */
public class Distinct extends Operator {

    // this is the base operator
    private Operator baseOperator;

    // this is the original list which Distinct will be used on.
    private List<Attribute> originalList;

    // size of a batch
    private int batchSize;

    private Vector<Integer> attributeIndex = new Vector<>();

    //boolean variable to check if end of line has been reached
    private boolean endOfLine = false;

    // object containing sorted
    private Sort sorted;

    // Input Batch's current element's index
    private int inIndex;

    // The input Batch
    Batch inBatch;
    // The output Batch
    Batch outBatch;

    // The very last tuple output
    private Tuple lastOutTuple = null;

    // this contains the number of buffers
    private int numOfBuffer;

    /**
     * Constructor for Distinct - initialise a distinct operation
     * @param baseOperator - self-explanatory
     * @param originalList - an original list in Vector in which Distinct will be used on to remove duplicates.
     */
    public Distinct(Operator baseOperator, List<Attribute> originalList) {
        super(OperatorType.DISTINCT);
        this.originalList = originalList;
        this.baseOperator = baseOperator;
    }

    @Override
    public boolean open() {
        /** set number of tuples per batch **/
        int tupleSize = schema.getTupleSize();
        int pageSize = Batch.getPageSize();
        batchSize = pageSize / tupleSize;
        int lengthOfOriginalList = originalList.size();

        for (int i = 0; i < lengthOfOriginalList; i++) {
            Attribute attribute = (Attribute) originalList.get(i);
            System.out.println((Attribute) originalList.get(i));
            attributeIndex.add(schema.indexOf(attribute));
        }

//        sorted = new Sort(baseOperator, originalList, Sort.Direction.ASC, BufferManager.getNumBuffer());
//        return sorted.open();
        return baseOperator.open();
    }

    /**
     * Read next tuple from operator
     * @return outBatch
     */
    @Override
    public Batch next() {
        if (endOfLine) {
            close();
            return null;
        }

        // do this if input Batch is null
        if (inBatch == null) {
//            inBatch = sorted.next();
            inBatch = baseOperator.next();
        }

        outBatch = new Batch(batchSize);
        // do loop if output Batch is still not full yet.
        while (!outBatch.isFull()) {
            if (inBatch == null || inBatch.size() <= inIndex) {
                endOfLine = true;
                break;
            }

            Tuple current = inBatch.elementAt(inIndex);
            if (lastOutTuple == null || checkTuplesNotEqual(lastOutTuple, current)) {
                outBatch.addRecord(current);
                lastOutTuple = current;
            }
            inIndex++;

            if (inIndex == batchSize) {
//                inBatch = sorted.next();
                inBatch = baseOperator.next();
                inIndex = 0;
            }
        }
        return outBatch;
    }

    private boolean checkTuplesNotEqual(Tuple tuple1, Tuple tuple2) {
        for (int index: attributeIndex) {
            int result = Tuple.compare(tuple1, tuple2, index);
            if (result != 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean close() {
//        return sorted.close();
        return baseOperator.close();
    }

    public Operator getBase() {
        return baseOperator;
    }

    public void setBase(Operator baseOperator) {
        this.baseOperator = baseOperator;
    }

    public Object clone() {
        Operator newBaseOperator = (Operator) baseOperator.clone();
        Vector<Attribute> newProjectList = new Vector<>();
        for (Attribute value : originalList) {
            Attribute attribute = (Attribute) value.clone();
            newProjectList.add(attribute);
        }

        Distinct newDistinctList = new Distinct(newBaseOperator, newProjectList);
        Schema newSchema = newBaseOperator.getSchema();
        newDistinctList.setSchema(newSchema);
        return newDistinctList;
    }
}