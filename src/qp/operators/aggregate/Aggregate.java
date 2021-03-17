package qp.operators.aggregate;

import qp.operators.Operator;
import qp.operators.OperatorType;

public class Aggregate extends Operator {
    protected final Operator base;
    protected final AggregateType aggregateType;

    protected Aggregate(Operator base, AggregateType aggregateType) {
        super(OperatorType.AGGREGATE);
        this.base = base;
        this.aggregateType = aggregateType;
    }
}
