package qp.operators.aggregate;

import qp.operators.Operator;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.util.List;

public class Avg extends Aggregate {
    private final int targetIndex;

    private final Batch pageWithAvg;
    private boolean isReturned;

    public Avg(Operator base, Attribute targetAttribute) {
        super(base, AggregateType.AVG);
        targetIndex = schema.indexOf(targetAttribute);
        pageWithAvg = new Batch(1);
        isReturned = false;
    }

    @Override
    public boolean open() {
        if (!base.open()) {
            return false;
        }

        float total = 0;
        int count = 0;
        Batch page;
        while ((page = base.next()) != null) {
            for (Tuple record : page.getRecords()) {
                total += (float) record.getData(targetIndex);
                count++;
            }
        }

        float avg = total / count;
        pageWithAvg.addRecord(new Tuple(List.of(avg)));

        return true;
    }

    @Override
    public Batch next() {
        if (isReturned) {
            return null;
        }
        isReturned = true;
        return pageWithAvg;
    }

    @Override
    public boolean close() {
        return true;
    }
}
