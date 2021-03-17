package qp.operators.aggregate;

public class SumAggregator implements Aggregator<Float> {
    private Float sum;

    public SumAggregator() {
        sum = 0f;
    }

    @Override
    public void include(Float value) {
        sum += value;
    }

    @Override
    public Float get() {
        return sum;
    }
}
