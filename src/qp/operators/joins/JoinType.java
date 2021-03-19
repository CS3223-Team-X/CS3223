package qp.operators.joins;

/**
 * The different join types.
 */
public class JoinType {
    public static final int PAGE_NESTED = 0;
    public static final int BLOCK_NESTED = 1;
    public static final int SORT_MERGE = 2;
    public static final int HASH = 3;

    /**
     * Gets the number of join types.
     * @return the number of join types
     */
    public static int numJoinTypes() {
        return 3;
    }
}
