package qp.operators.joins;

/**
 * The Page Nested Join algorithm.
 */
public class PageNestedJoin extends NestedJoin {
    /**
     * By definition, the Page Nested algorithm uses a left
     * buffer size of 1 page.
     */
    private static final int LEFT_BUFFER_SIZE = 1;

    /**
     * Applies the Page Nested Join algorithm to a logical join
     * operator.
     *
     * @param join A logical join operator without an algorithm
     *             applied
     */
    public PageNestedJoin(Join join) {
        super(join, LEFT_BUFFER_SIZE);
    }
}
