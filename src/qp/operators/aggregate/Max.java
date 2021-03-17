package qp.operators.aggregate;

import qp.operators.Operator;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.List;

public class Max extends Aggregate {
    private final Attribute targetAttribute;
    private final int targetIndex;

    private final Batch pageWithMaxRecord;
    private boolean isReturned;

    public Max(Operator base, Attribute targetAttribute) {
        super(base, AggregateType.MAX);
        this.targetAttribute = targetAttribute;
        targetIndex = schema.indexOf(this.targetAttribute);

        pageWithMaxRecord = new Batch(1);
        isReturned = false;
    }

    @Override
    public boolean open() {
        if (!base.open()) {
            return false;
        }

        Batch page = base.next();
        if (page == null) {
            return false;
        }

        Tuple maxRecord = page.getRecord(0);
        do {
            List<Tuple> records = page.getRecords();
            for (Tuple record : records) {
                if (Tuple.compare(maxRecord, record, targetIndex) > 0) {
                    maxRecord = record;
                }
            }
        } while ((page = base.next()) != null);

        pageWithMaxRecord.addRecord(maxRecord);

        return true;
    }

    @Override
    public Batch next() {
        if (isReturned) {
            return null;
        }
        isReturned = true;
        return pageWithMaxRecord;
    }

    @Override
    public boolean close() {
        // do nothing
        return true;
    }

    @Override
    public Object clone() {
        Max newMax = new Max((Operator) base.clone(), targetAttribute);
        newMax.setSchema((Schema) schema.clone());
        return newMax;
    }
}
