package qp.operators;

/**
 * The Block Nested Join algorithm.
 */
public class BlockNestedJoin extends NestedJoin {
    /**
     * Applies the Block Nested Join algorithm to a logical join
     * operator.
     * <p>
     * The block size is the remaining number of pages after
     * allocating 1 for the right input buffer and 1 for the
     * output buffer.
     *
     * @param join A logical join operator without an algorithm
     *             applied
     * @param blockSize The block size to use
     */
    public BlockNestedJoin(Join join, int blockSize) {
        super(join, blockSize);
    }
}
