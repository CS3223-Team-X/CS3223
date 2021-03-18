/**
 * This is base class for all the join operators
 **/

package qp.operators.joins;

import qp.operators.Operator;
import qp.operators.OperatorType;
import qp.utils.Condition;
import qp.utils.Schema;

import java.util.ArrayList;
import java.util.List;

public class Join extends Operator {
    protected static int CURSOR_START = 0;

    protected Operator left;                       // Left child
    protected Operator right;                      // Right child
    protected List<Condition> joinConditions;  // List of join conditions
    protected int numBuff;                         // Number of buffers available
    protected int joinType;                        // JoinType.NestedJoin/SortMerge/HashJoin
    protected int nodeIndex;                       // Each join node is given a number

    public Join(Operator left, Operator right) {
        super(OperatorType.JOIN);
        this.left = left;
        this.right = right;
        joinConditions = new ArrayList<>();
    }

    public Join(Operator left, Operator right, Condition condition) {
        super(OperatorType.JOIN);
        this.left = left;
        this.right = right;
        joinConditions = new ArrayList<>();
        joinConditions.add(condition);
    }

    public Join(Operator left, Operator right, List<Condition> joinConditions) {
        super(OperatorType.JOIN);
        this.left = left;
        this.right = right;
        this.joinConditions = joinConditions;
    }

    public int getNumBuff() {
        return numBuff;
    }

    public void setNumBuff(int num) {
        this.numBuff = num;
    }

    public int getNodeIndex() {
        return nodeIndex;
    }

    public void setNodeIndex(int num) {
        this.nodeIndex = num;
    }

    public int getJoinType() {
        return joinType;
    }

    public void setJoinType(int type) {
        this.joinType = type;
    }

    public Operator getLeft() {
        return left;
    }

    public void setLeft(Operator left) {
        this.left = left;
    }

    public Operator getRight() {
        return right;
    }

    public void setRight(Operator right) {
        this.right = right;
    }

    public Condition getCondition() {
        assert (joinConditions.size() > 0);
        return joinConditions.get(0);
    }

    public void setCondition(Condition condition) {
        joinConditions = new ArrayList<>();
        joinConditions.add(condition);
    }

    public List<Condition> getJoinConditions() {
        return joinConditions;
    }

    public void setJoinConditions(ArrayList<Condition> joinConditions) {
        this.joinConditions = joinConditions;
    }

    public void addCondition(Condition condition) {
        joinConditions.add(condition);
    }

    @Override
    public Object clone() {
        Operator newLeft = (Operator) left.clone();
        Operator newRight = (Operator) right.clone();
        List<Condition> newJoinConditions = new ArrayList<>();
        for (Condition joinCondition : joinConditions) {
            newJoinConditions.add((Condition) joinCondition.clone());
        }

        Join newJoin = new Join(newLeft, newRight, newJoinConditions);
        Schema newSchema = newLeft.getSchema().joinWith(newRight.getSchema());
        newJoin.setSchema(newSchema);
        newJoin.setJoinType(joinType);
        newJoin.setNodeIndex(nodeIndex);
        newJoin.setNumBuff(numBuff);

        return newJoin;
    }
}
