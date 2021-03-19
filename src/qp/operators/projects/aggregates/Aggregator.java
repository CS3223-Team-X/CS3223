package qp.operators.projects.aggregates;

public interface Aggregator<E> {
    void include(E value);

    E get();
}
