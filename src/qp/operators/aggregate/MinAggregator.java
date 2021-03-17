package qp.operators.aggregate;

public class MinAggregator implements Aggregator<Float> {
    private Float minValue;

    public MinAggregator() {
        minValue = Float.MAX_VALUE;
    }

    @Override
    public void include(Float value) {
        if (value < minValue) {
            minValue = value;
        }
    }

    @Override
    public Float get() {
        return minValue;
    }
}
