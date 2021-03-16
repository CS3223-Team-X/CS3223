package qp.operators;

import qp.utils.Batch;

public class OrderBy extends Operator {
    private final Sort sort;

    public OrderBy(Sort sort) {
        super(OperatorType.SORT);
        this.sort = sort;
    }

    @Override
    public boolean open() {
        return sort.open();
    }

    @Override
    public Batch next() {
        return sort.next();
    }

    @Override
    public boolean close() {
        return sort.close();
    }
}
