package qp.operators.projects.aggregate;

public interface Aggregator<E> {
    void include(E value);

    E get();
}
