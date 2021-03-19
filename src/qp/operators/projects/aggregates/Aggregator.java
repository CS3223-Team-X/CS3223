package qp.operators.projects.aggregates;

/**
 * The interface which all aggregate functions implement.
 *
 * @param <E> The type of input and output
 */
public interface Aggregator<E> {
    /**
     * Includes the specified value as part of the aggregation.
     *
     * @param value The value to be aggregated in
     */
    void include(E value);

    /**
     * Gets the aggregated value.
     *
     * @return the aggregated value
     */
    E get();
}
