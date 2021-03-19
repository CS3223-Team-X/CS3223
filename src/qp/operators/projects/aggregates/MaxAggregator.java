package qp.operators.projects.aggregates;

/**
 * An aggregator to find the maximum in a series of values.
 */
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
