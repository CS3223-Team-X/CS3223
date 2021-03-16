/**
 * This is base class for all the operators
 **/
package qp.operators;

import qp.utils.Batch;
import qp.utils.Schema;

public class Operator {
    protected int opType;     // Whether it is OpType.SELECT/ Optype.PROJECT/OpType.JOIN
    protected Schema schema;  // Schema of the result at this operator

    public Operator(int type) {
        this.opType = type;
    }

    public int getOpType() {
        return opType;
    }

    public void setOpType(int type) {
        this.opType = type;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public boolean open() {
        System.err.println("Abstract interface cannot be used.");
        System.exit(1);
        return true;
    }

    public Batch next() {
        System.err.println("Abstract interface cannot be used.");
        System.exit(1);
        return null;
    }

    public boolean close() {
        return true;
    }

    public Object clone() {
        return new Operator(opType);
    }
}
