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
    private int inputBufferElementIndex;

    // The input Batch
    Batch inputBatch;
    // The output Batch
    Batch outputBatch;

    // The very last tuple output
    private Tuple lastOutputTuple = null;

    /**
     * Constructor for Distinct - initialise a distinct operation
     *
     * @param baseOperator - self-explanatory
     * @param originalList - an original list in Vector in which Distinct will be used on to remove duplicates.
     */
    public Distinct(Operator baseOperator, List<Attribute> originalList) {
        super(OperatorType.DISTINCT);
        this.originalList = originalList;
        this.baseOperator = baseOperator;
    }

    /**
     * This method will run when Distinct operator is called.
     * It will sort the inputs.
     * @return True if it has been sorted
     */
    @Override
    public boolean open() {
        /** set number of tuples per batch **/
        int tupleSize = schema.getTupleSize();
        int pageSize = Batch.getPageSize();
        batchSize = pageSize / tupleSize;
        int lengthOfOriginalList = originalList.size();

        for (int i = 0; i < lengthOfOriginalList; i++) {
            Attribute attribute = originalList.get(i);
            attributeIndex.add(schema.indexOf(attribute));
        }

        sorted = new Sort(baseOperator, originalList, Sort.Direction.ASC, BufferManager.getNumBuffer());
        return sorted.open();
    }

    /**
     * Read next tuple from operator
     *
     * @return outBatch
     */
    @Override
    public Batch next() {
        inputBufferElementIndex = 0;
        // finished parsing already, can close and return null
        if (endOfLine) {
            close();
            return null;
        }

        // if there is nothing in the input buffer, do next to see next
        if (inputBatch == null) {
            inputBatch = sorted.next();
        }

        // initialise output buffer to store the results
        outputBatch = new Batch(batchSize);

        // do loop if output Batch is still not full yet.
        while (!outputBatch.isFull()) {

            //check if it has finished
            if (inputBatch == null) {
                endOfLine = true;
                break;
            }

            //get current tuple for comparison
            Tuple current = inputBatch.elementAt(inputBufferElementIndex);

            // do the comparison before deciding to add to output batch
            if (lastOutputTuple == null || checkIfTuplesAreEqual(lastOutputTuple, current)) {
                outputBatch.addRecord(current);
                lastOutputTuple = current;
            }

            inputBufferElementIndex++;
            //if input batch is now full, restart the index and start a new inputBatch
            if (inputBufferElementIndex == inputBatch.size()) {
                inputBatch = sorted.next();
                inputBufferElementIndex = 0;
            }
        }
        return outputBatch;
    }

    /**
     * This method compares tuples to check if they are duplicates.
     * @param firstTuple for comparison
     * @param secondTuple for comparisons
     * @return True if they are the same, false otherwise
     */
    private boolean checkIfTuplesAreEqual(Tuple firstTuple, Tuple secondTuple) {
        for (int indexNumber : attributeIndex) {
            int result = Tuple.compare(firstTuple, secondTuple, indexNumber);
            if (result != 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Method is a wrapper for the closing of this and the next operator down the query plan tree
     * @return boolean true or false
     */
    @Override
    public boolean close() {
        return sorted.close();
    }

    /**
     * Method will get baseOperator
     * @return baseOperator
     */
    public Operator getBase() {
        return baseOperator;
    }

    /**
     * Method will set baseOperator
     * @param baseOperator
     */
    public void setBase(Operator baseOperator) {
        this.baseOperator = baseOperator;
    }


    /**
     * Method will clone the results
     * @return the new list that is distinct (no duplicates)
     */
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