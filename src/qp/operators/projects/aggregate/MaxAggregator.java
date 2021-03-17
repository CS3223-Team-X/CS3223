package qp.operators.projects.aggregate;

public class MaxAggregator implements Aggregator<Float> {
    private Float maxValue;

    public MaxAggregator() {
        maxValue = Float.MIN_VALUE;
    }

    @Override
    public void include(Float value) {
        if (value > maxValue) {
            maxValue = value;
        }
    }

    @Override
    public Float get() {
        return maxValue;
    }
}
