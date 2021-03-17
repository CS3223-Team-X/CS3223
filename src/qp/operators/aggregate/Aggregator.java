package qp.operators.aggregate;

import qp.operators.Operator;
import qp.operators.OperatorType;

public interface Aggregator<E> {
    void include(E value);

    E get();
}
