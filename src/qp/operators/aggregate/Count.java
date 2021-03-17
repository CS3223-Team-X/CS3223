package qp.operators.aggregate;

import qp.operators.Operator;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.util.List;

public class Count extends Aggregate {
    private final Batch pageWithCount;
    private boolean isReturned;

    public Count(Operator base) {
        super(base, AggregateType.COUNT);
        pageWithCount = new Batch(1);
        isReturned = false;
    }

    @Override
    public boolean open() {
        if (!base.open()) {
            return false;
        }

        int count = 0;
        Batch page;
        while ((page = base.next()) != null) {
            count += page.size();
        }

        pageWithCount.addRecord(new Tuple(List.of(count)));

        return true;
    }

    @Override
    public Batch next() {
        if (isReturned) {
            return null;
        }
        isReturned = true;
        return pageWithCount;
    }

    @Override
    public boolean close() {
        return true;
    }
}
