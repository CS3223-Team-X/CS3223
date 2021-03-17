package qp.operators.aggregate;

import qp.operators.Operator;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.List;

public class Min extends Aggregate {
    private final Attribute targetAttribute;
    private final int targetIndex;

    private final Batch pageWithMinRecord;
    private boolean isReturned;

    public Min(Operator base, Attribute targetAttribute) {
        super(base, AggregateType.MIN);
        this.targetAttribute = targetAttribute;
        targetIndex = schema.indexOf(this.targetAttribute);

        pageWithMinRecord = new Batch(1);
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

        Tuple minRecord = page.getRecord(0);
        do {
            List<Tuple> records = page.getRecords();
            for (Tuple record : records) {
                if (Tuple.compare(minRecord, record, targetIndex) < 0) {
                    minRecord = record;
                }
            }
        } while ((page = base.next()) != null);

        pageWithMinRecord.addRecord(minRecord);

        return true;
    }

    @Override
    public Batch next() {
        if (isReturned) {
            return null;
        }
        isReturned = true;
        return pageWithMinRecord;
    }

    @Override
    public boolean close() {
        // do nothing
        return true;
    }

    @Override
    public Object clone() {
        Min newMin = new Min((Operator) base.clone(), targetAttribute);
        newMin.setSchema((Schema) schema.clone());
        return newMin;
    }
}
