package qp.operators.aggregate;

import qp.operators.Operator;
import qp.operators.OperatorType;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.List;

public class Min extends Operator {
    private final Operator base;
    private final Attribute targetAttribute;
    private final int targetIndex;

    private Batch pageWithMinRecord;
    private boolean isReturned;

    public Min(Operator base, Attribute targetAttribute) {
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

        Tuple minRecord = page.getRecord(0);
        do {
            List<Tuple> records = page.getRecords();
            for (Tuple record : records) {
                if (Tuple.compare(minRecord, record, targetIndex) < 0) {
                    minRecord = record;
                }
            }
        } while ((page = base.next()) != null);

        pageWithMinRecord = new Batch(1);
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
