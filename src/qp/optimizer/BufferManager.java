/**
 * simple buffer manager that distributes the buffers equally among all the join operators
 **/

package qp.optimizer;

public class BufferManager {
    private static int numBuffer;
    private static int numJoin;

    public static void init(int numBuffers, int numJoins) {
        numBuffer = numBuffers;
        numJoin = numJoins;
    }

    public static int getNumBuffer() {
        return numBuffer;
    }

    public static int getBuffersPerJoin() {
        return numBuffer / numJoin;
    }
}
