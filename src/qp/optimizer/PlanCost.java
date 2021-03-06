/**
 * This method calculates the cost of the generated plans
 * also estimates the statistics of the result relation
 **/

package qp.optimizer;

import qp.operators.*;
import qp.operators.joins.Join;
import qp.operators.joins.JoinType;
import qp.operators.projects.Project;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Schema;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

public class PlanCost {

    long cost;
    long numtuple;

    /**
     * If buffers are not enough for a selected join
     * * then this plan is not feasible and return
     * * a cost of infinity
     **/
    boolean isFeasible;

    /**
     * Hashtable stores mapping from Attribute name to
     * * number of distinct values of that attribute
     **/
    HashMap<Attribute, Long> ht;


    public PlanCost() {
        ht = new HashMap<>();
        cost = 0;
    }

    /**
     * Returns the cost of the plan
     **/
    public long getCost(Operator root) {
        cost = 0;
        isFeasible = true;
        numtuple = calculateCost(root);
        if (isFeasible) {
            return cost;
        } else {
            System.out.println("notFeasible");
            return Long.MAX_VALUE;
        }
    }

    /**
     * Get number of tuples in estimated results
     **/
    public long getNumTuples() {
        return numtuple;
    }


    /**
     * Returns number of tuples in the root
     **/
    private long calculateCost(Operator node) {
        if (node.getOpType() == OperatorType.JOIN) {
            return getStatistics((Join) node);
        } else if (node.getOpType() == OperatorType.SELECT) {
            return getStatistics((Select) node);
        } else if (node.getOpType() == OperatorType.PROJECT) {
            return getStatistics((Project) node);
        } else if (node.getOpType() == OperatorType.SCAN) {
            return getStatistics((Scan) node);
        } else if (node.getOpType() == OperatorType.ORDER) {
            return getStatistics((OrderBy) node);
        } else if (node.getOpType() == OperatorType.DISTINCT) {
            return getStatistics((Distinct) node);
        }
        System.out.println("operator is not supported");
        isFeasible = false;
        return 0;
    }

    private long getStatistics(Distinct node) {
        long tuples = calculateCost(node.getBase());
        long tupleSize = node.getBase().getSchema().getTupleSize();
        long capacity = Math.max(1, Batch.getPageSize() / tupleSize);
        long pages = (long) Math.ceil(((double) tuples) / (double) capacity);
        long numOfPasses = (long) (1 + Math.ceil(Math.log(Math.ceil(pages / (double) (BufferManager.getNumBuffer())) / Math.log(BufferManager.getNumBuffer() - 1))));
        long distinctCost = (long) (2 * pages * (numOfPasses));
        long outtuples = (long) Math.ceil(tuples);
        cost += distinctCost;
        return outtuples;
    }

    /**
     * Projection will not change any statistics
     * * No cost involved as done on the fly
     **/
    private long getStatistics(Project node) {
        return calculateCost(node.getBase());
    }

    /**
     * Calculates the statistics and cost of join operation
     **/
    private long getStatistics(Join node) {
        long lefttuples = calculateCost(node.getLeft());
        long righttuples = calculateCost(node.getRight());

        if (!isFeasible) {
            return 0;
        }

        Schema leftschema = node.getLeft().getSchema();
        Schema rightschema = node.getRight().getSchema();

        /** Get size of the tuple in output & correspondigly calculate
         ** buffer capacity, i.e., number of tuples per page **/
        long tupleSize = node.getSchema().getTupleSize();
        long outcapacity = Math.max(1, Batch.getPageSize() / tupleSize);
        long leftTupleSize = leftschema.getTupleSize();
        long leftCapacity = Math.max(1, Batch.getPageSize() / leftTupleSize);
        long rightTupleSize = rightschema.getTupleSize();
        long rightCapacity = Math.max(1, Batch.getPageSize() / rightTupleSize);
        long leftPages = (long) Math.ceil(((double) lefttuples) / (double) leftCapacity);
        long rightPages = (long) Math.ceil(((double) righttuples) / (double) rightCapacity);

        double tuples = (double) lefttuples * righttuples;
        for (Condition con : node.getJoinConditions()) {
            Attribute leftjoinAttr = con.getLhs();
            Attribute rightjoinAttr = (Attribute) con.getRhs();
            int leftattrind = leftschema.indexOf(leftjoinAttr);
            int rightattrind = rightschema.indexOf(rightjoinAttr);
            leftjoinAttr = leftschema.getAttribute(leftattrind);
            rightjoinAttr = rightschema.getAttribute(rightattrind);

            /** Number of distinct values of left and right join attribute **/
            long leftattrdistn = ht.get(leftjoinAttr);
            long rightattrdistn = ht.get(rightjoinAttr);
            tuples /= (double) Math.max(leftattrdistn, rightattrdistn);
            long mindistinct = Math.min(leftattrdistn, rightattrdistn);
            ht.put(leftjoinAttr, mindistinct);
            ht.put(rightjoinAttr, mindistinct);
        }
        long outtuples = (long) Math.ceil(tuples);

        /** Calculate the cost of the operation **/
        int joinType = node.getJoinType();
        long numbuff = BufferManager.getBuffersPerJoin();
        long joinCost;

        switch (joinType) {
            case JoinType.PAGE_NESTED:
                joinCost = leftPages * rightPages;
                break;
            case JoinType.BLOCK_NESTED:
                joinCost = leftPages + (long) Math.ceil(leftPages/ (double) (BufferManager.getBuffersPerJoin() - 2) ) * rightPages;
                break;
            case JoinType.SORT_MERGE:
                long numOfPasses = (long) (1 + Math.ceil(Math.log(Math.ceil(rightPages / (double) (BufferManager.getNumBuffer())) / Math.log(BufferManager.getNumBuffer() - 1))));
                joinCost = leftPages + rightPages + (long) (2 * rightPages * (numOfPasses));
                break;
            default:
                System.out.println("join type is not supported");
                return 0;
        }
        cost = cost + joinCost;

        return outtuples;
    }

    /**
     * Find number of incoming tuples, Using the selectivity find # of output tuples
     * * And statistics about the attributes
     * * Selection is performed on the fly, so no cost involved
     **/
    private long getStatistics(Select node) {
        long intuples = calculateCost(node.getBase());
        if (!isFeasible) {
            System.out.println("notFeasible");
            return Long.MAX_VALUE;
        }

        Condition con = node.getCondition();
        Schema schema = node.getSchema();
        Attribute attr = con.getLhs();
        int index = schema.indexOf(attr);
        Attribute fullattr = schema.getAttribute(index);
        int exprtype = con.getExprType();

        /** Get number of distinct values of selection attributes **/
        long numdistinct = intuples;
        Long temp = ht.get(fullattr);
        numdistinct = temp.longValue();

        long outtuples;
        /** Calculate the number of tuples in result **/
        if (exprtype == Condition.EQUAL) {
            outtuples = (long) Math.ceil((double) intuples / (double) numdistinct);
        } else if (exprtype == Condition.NOTEQUAL) {
            outtuples = (long) Math.ceil(intuples - ((double) intuples / (double) numdistinct));
        } else {
            outtuples = (long) Math.ceil(0.5 * intuples);
        }

        /** Modify the number of distinct values of each attribute
         ** Assuming the values are distributed uniformly along entire
         ** relation
         **/
        for (int i = 0; i < schema.getNumCols(); ++i) {
            Attribute attri = schema.getAttribute(i);
            long oldvalue = ht.get(attri);
            long newvalue = (long) Math.ceil(((double) outtuples / (double) intuples) * oldvalue);
            ht.put(attri, outtuples);
        }
        return outtuples;
    }

    /**
     * The statistics file <tablename>.stat to find the statistics
     * * about that table;
     * * This table contains number of tuples in the table
     * * number of distinct values of each attribute
     **/
    private long getStatistics(Scan node) {
        String tablename = node.getTabName();
        String filename = tablename + ".stat";
        Schema schema = node.getSchema();
        int numAttr = schema.getNumCols();
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(filename));
        } catch (IOException io) {
            System.out.println("Error in opening file" + filename);
            System.exit(1);
        }
        String line = null;

        // First line = number of tuples
        try {
            line = in.readLine();
        } catch (IOException io) {
            System.out.println("Error in readin first line of " + filename);
            System.exit(1);
        }
        StringTokenizer tokenizer = new StringTokenizer(line);
        if (tokenizer.countTokens() != 1) {
            System.out.println("incorrect format of statastics file " + filename);
            System.exit(1);
        }
        String temp = tokenizer.nextToken();
        long numtuples = Long.parseLong(temp);
        try {
            line = in.readLine();
        } catch (IOException io) {
            System.out.println("error in reading second line of " + filename);
            System.exit(1);
        }
        tokenizer = new StringTokenizer(line);
        if (tokenizer.countTokens() != numAttr) {
            System.out.println("incorrect format of statastics file " + filename);
            System.exit(1);
        }
        for (int i = 0; i < numAttr; ++i) {
            Attribute attr = schema.getAttribute(i);
            temp = tokenizer.nextToken();
            Long distinctValues = Long.valueOf(temp);
            ht.put(attr, distinctValues);
        }

        /** Number of tuples per page**/
        long tuplesize = schema.getTupleSize();
        long pagesize = Math.max(Batch.getPageSize() / tuplesize, 1);
        long numpages = (long) Math.ceil((double) numtuples / (double) pagesize);

        cost = cost + numpages;

        try {
            in.close();
        } catch (IOException io) {
            System.out.println("error in closing the file " + filename);
            System.exit(1);
        }
        return numtuples;
    }

    private long getStatistics(OrderBy node) {
        long tuples = calculateCost(node.getBase());
        long tupleSize = node.getBase().getSchema().getTupleSize();
        long capacity = Math.max(1, Batch.getPageSize() / tupleSize);
        long pages = (long) Math.ceil(((double) tuples) / (double) capacity);
        long numOfPasses = (long) (1 + Math.ceil(Math.log(Math.ceil(pages / (double) (BufferManager.getNumBuffer())) / Math.log(BufferManager.getNumBuffer() - 1))));
        long orderByCost = (long) (2 * pages * (numOfPasses));
        long outtuples = (long) Math.ceil(tuples);
        cost += orderByCost;
        return outtuples;
    }

}











