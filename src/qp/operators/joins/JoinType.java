/**
 * Enumeration of join algorithm types
 * Change this class depending on actual algorithms
 * you have implemented in your query processor
 **/

package qp.operators.joins;

public class JoinType {
    public static final int PAGE_NESTED = 0;
    public static final int BLOCK_NESTED = 1;
    public static final int SORT_MERGE = 2;
    public static final int HASH = 3;

    public static int numJoinTypes() {
        return 1;
    }
}
