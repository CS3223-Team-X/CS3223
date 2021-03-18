package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.Batch;

import java.util.ArrayList;
import java.util.List;

public class OrderBy extends Operator {
    private Operator base;

    private Sort sort;
    private final List<Attribute> orderByAttributes;
    private final Sort.Direction sortDirection;

    public OrderBy(Operator base, List<Attribute> orderByAttributes, Sort.Direction sortDirection) {
        super(OperatorType.ORDER);
        this.base = base;

        this.orderByAttributes = orderByAttributes;
        this.sortDirection = sortDirection;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    @Override
    public boolean open() {
        sort = new Sort(base, orderByAttributes, sortDirection, BufferManager.getNumBuffer());
        sort.setSchema(base.getSchema());
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
        List<Attribute> newOrderByAttributes = new ArrayList<>();
        for (Attribute orderByAttribute : orderByAttributes) {
            newOrderByAttributes.add((Attribute) orderByAttribute.clone());
        }
        OrderBy newOrderBy = new OrderBy((Operator) base.clone(), newOrderByAttributes, sortDirection);
        newOrderBy.setSchema(base.getSchema());
        return newOrderBy;
    }
}
