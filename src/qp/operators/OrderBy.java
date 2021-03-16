package qp.operators;

import qp.utils.Batch;

public class OrderBy extends Operator {
    private Operator base;
    private final Sort sort;

    public OrderBy(Operator base, Sort sort) {
        super(OperatorType.ORDER);
        this.base = base;
        this.sort = sort;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
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

    @Override
    public Object clone() {
        return new OrderBy((Operator) base.clone(), (Sort) sort.clone());
    }
}
