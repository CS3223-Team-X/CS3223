package qp.operators.aggregate;

public class AvgAggregator implements Aggregator<Float> {
    private long count;
    private Float totalValue;

    public AvgAggregator() {
        count = 0L;
        totalValue = 0f;
    }

    @Override
    public void include(Float value) {
        totalValue += value;
        count += 1;
    }

    @Override
    public Float get() {
        return totalValue / count;
    }
}
