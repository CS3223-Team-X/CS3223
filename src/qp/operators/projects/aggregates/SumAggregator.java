package qp.operators.projects.aggregates;

/**
 * An aggregator to compute the sum of a series of values.
 */
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
