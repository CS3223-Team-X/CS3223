/**
 * Tuple container class
 **/

package qp.utils;

import java.util.*;
import java.io.*;
import java.util.stream.Collectors;

/**
 * Tuple - a simple object which holds an ArrayList of data
 */
public class Tuple implements Serializable {
    private final List<Object> data;

    public Tuple(List<Object> data) {
        this.data = data;
    }

    public List<Object> getData() {
        return data;
    }

    public Object getData(int index) {
        return data.get(index);
    }

    /**
     * Checks whether the join condition is satisfied or not with one condition
     * * before performing actual join operation
     **/
    public boolean checkJoin(Tuple right, int leftindex, int rightindex) {
        Object leftData = getData(leftindex);
        Object rightData = right.getData(rightindex);
        if (leftData.equals(rightData))
            return true;
        else
            return false;
    }

    /**
     * Checks whether the join condition is satisfied or not with multiple conditions
     * * before performing actual join operation
     **/
    public boolean checkJoin(Tuple right, List<Integer> leftindex, List<Integer> rightindex) {
        if (leftindex.size() != rightindex.size()) {
            return false;
        }
        for (int i = 0; i < leftindex.size(); ++i) {
            Object leftData = getData(leftindex.get(i));
            Object rightData = right.getData(rightindex.get(i));
            if (!leftData.equals(rightData)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Joining two tuples without duplicate column elimination
     **/
    public Tuple joinWith(Tuple right) {
        ArrayList<Object> newData = new ArrayList<>(this.getData());
        newData.addAll(right.getData());
        return new Tuple(newData);
    }

    /**
     * Compare two tuples in the same table on given attribute
     **/
    public static int compare(Tuple left, Tuple right, int index) {
        return compareTuples(left, right, index, index);
    }

    public static int compare(Tuple t1, Tuple t2, List<Integer> sortIndices) {
        for (int sortIndex : sortIndices) {
            Object data1 = t1.getData(sortIndex);
            Object data2 = t2.getData(sortIndex);
            if (data1.equals(data2)) {
                continue;
            }
            return compareByTypes(data1, data2);
        }
        return 0;
    }

    /**
     * Comparing tuples in different tables, used for join condition checking
     **/
    public static int compareTuples(Tuple left, Tuple right, int leftIndex, int rightIndex) {
        Object leftData = left.getData(leftIndex);
        Object rightData = right.getData(rightIndex);
        return compareByTypes(leftData, rightData);
    }

    /**
     * Comparing tuples in different tables with multiple conditions, used for join condition checking
     **/
    public static int compareTuples(Tuple left, Tuple right, ArrayList<Integer> leftIndex, ArrayList<Integer> rightIndex) {
        if (leftIndex.size() != rightIndex.size()) {
            System.out.println("Tuple: Unknown comparison of the tuples");
            System.exit(1);
            return 0;
        }

        for (int i = 0; i < leftIndex.size(); ++i) {
            Object leftData = left.getData(leftIndex.get(i));
            Object rightData = right.getData(rightIndex.get(i));
            if (leftData.equals(rightData)) {
                continue;
            }
            return compareByTypes(leftData, rightData);
        }

        return 0;
    }

    private static int compareByTypes(Object data1, Object data2) {
        if (data1 instanceof Integer) {
            return ((Integer) data1).compareTo((Integer) data2);
        } else if (data1 instanceof String) {
            return ((String) data1).compareTo((String) data2);
        } else if (data1 instanceof Float) {
            return ((Float) data1).compareTo((Float) data2);
        } else {
            System.out.println("Tuple: Unknown comparison of tuples");
            System.exit(1);
            return 0;
        }
    }

    @Override
    public String toString() {
        List<String> dataStrings = data.stream().map(String::valueOf).collect(Collectors.toUnmodifiableList());
        return String.join(", ", dataStrings);
    }
}
