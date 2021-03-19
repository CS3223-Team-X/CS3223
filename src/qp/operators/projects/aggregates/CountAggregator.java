package qp.operators.projects.aggregates;

public class CountAggregator implements Aggregator<Integer> {
    public static final Integer DONT_CARE = -1;
    private int quantity;

    public CountAggregator() {
        quantity = 0;
    }

    @Override
    public void include(Integer value) {
        quantity++;
    }

    @Override
    public Integer get() {
        return quantity;
    }
}
