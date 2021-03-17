package qp.operators.aggregate;

import qp.operators.Operator;
import qp.operators.OperatorType;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.List;

public class Max extends Operator {
    private final Operator base;
    private final Attribute targetAttribute;
    private final int targetIndex;

    private Batch pageWithMaxRecord;
    private boolean isReturned;

    public Max(Operator base, Attribute targetAttribute) {
        super(OperatorType.AGGREGATE);
        this.base = base;
        this.targetAttribute = targetAttribute;
        targetIndex = schema.indexOf(this.targetAttribute);

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

        pageWithMaxRecord = new Batch(1);
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
