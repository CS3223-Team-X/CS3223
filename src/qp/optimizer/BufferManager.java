package qp.optimizer;

/**
 * Contains information related to buffers.
 */
public class BufferManager {
    /**
     * The number of buffer pages allocated to the system.
     */
    private static int numBuffer;
    /**
     * The number of joins involved.
     */
    private static int numJoin;

    /**
     * Initialises the {@code BufferManager.}
     * <p>
     * This method should be called only once.
     *
     * @param numBuffers The number of buffer pages allocated to
     *                   the system
     * @param numJoins The number of joins involved
     */
    public static void init(int numBuffers, int numJoins) {
        numBuffer = numBuffers;
        numJoin = numJoins;
    }

    public static int getNumBuffer() {
        return numBuffer;
    }

    /**
     * Computes the number of buffer pages each join can be
     * allocated with.
     *
     * @return The number of buffer pages per join
     */
    public static int getBuffersPerJoin() {
        return numBuffer / numJoin;
    }
}
