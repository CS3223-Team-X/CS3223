/**
 * This class to print various information
 **/

package qp.operators;

import qp.operators.joins.Join;
import qp.operators.joins.JoinType;
import qp.operators.projects.Project;
import qp.utils.*;

public class Debug {

    /**
     * print the attribute
     **/
    public static void PPrint(Attribute attr) {
        String[] aggregates = new String[]{"", "MAX", "MIN", "SUM", "COUNT", "AVG"};

        int aggregate = attr.getAggType();
        String tabname = attr.getTabName();
        String colname = attr.getColName();
        if (aggregate == 0) {
            System.out.print(tabname + "." + colname);
        } else {
            System.out.print(aggregates[aggregate] + "(" + tabname + "." + colname + ")  ");
        }
    }

    /**
     * print the condition
     **/
    public static void PPrint(Condition con) {
        Attribute lhs = con.getLhs();
        Object rhs = con.getRhs();
        int exprtype = con.getExprType();
        PPrint(lhs);
        switch (exprtype) {
            case Condition.LESSTHAN:
                System.out.print("<");
                break;
            case Condition.GREATERTHAN:
                System.out.print(">");
                break;
            case Condition.LTOE:
                System.out.print("<=");
                break;
            case Condition.GTOE:
                System.out.print(">=");
                break;
            case Condition.EQUAL:
                System.out.print("==");
                break;
            case Condition.NOTEQUAL:
                System.out.print("!=");
                break;
        }

        if (con.getOpType() == Condition.JOIN) {
            PPrint((Attribute) rhs);
        } else if (con.getOpType() == Condition.SELECT) {
            System.out.print((String) rhs);
        }
    }


    /**
     * print schema
     **/
    public static void PPrint(Schema schema) {
        System.out.println();
        for (int i = 0; i < schema.getNumCols(); i++) {
            Attribute attr = schema.getAttribute(i);
            PPrint(attr);
        }
        System.out.println();
    }


    /**
     * print a node in plan tree
     **/
    public static void PPrint(Operator node) {
        int optype = node.getOpType();

        switch (optype) {
            case OperatorType.JOIN:
                int exprtype = ((Join) node).getJoinType();
                switch (exprtype) {
                    case JoinType.PAGE_NESTED:
                        System.out.print("PageNested(");
                        break;
                    case JoinType.BLOCK_NESTED:
                        System.out.print("BlockNested(");
                        break;
                    case JoinType.SORT_MERGE:
                        System.out.print("SortMerge(");
                        break;
                    case JoinType.HASH:
                        System.out.print("Hash(");
                        break;
                }
                PPrint(((Join) node).getLeft());
                System.out.print("  [");
                PPrint(((Join) node).getCondition());
                System.out.print("]  ");
                PPrint(((Join) node).getRight());
                System.out.print(")");

                break;
            case OperatorType.SELECT:
                System.out.print("Select(");
                PPrint(((Select) node).getBase());
                System.out.print("  '");
                PPrint(((Select) node).getCondition());
                System.out.print(")");

                break;
            case OperatorType.PROJECT:
                System.out.print("Project(");
                PPrint(((Project) node).getBase());
                System.out.print(")");

                break;
            case OperatorType.SCAN:
                System.out.print(((Scan) node).getTabName());
                break;

            case OperatorType.ORDER:
                String sortDirection;
                switch (((OrderBy) node).getSortDirection()) {
                    case ASC:
                        sortDirection = "ASC";
                        break;
                    case DSC:
                        sortDirection = "DSC";
                        break;
                    default:
                        throw new RuntimeException();
                }

                System.out.println("Sort by " + sortDirection + " (");
                PPrint(((OrderBy) node).getBase());
                System.out.println(")");
                break;

            case OperatorType.DISTINCT:
                System.out.println("Distinct(");
                PPrint(((Distinct) node).getBase());
                System.out.println(")");
                break;
            default:
                throw new RuntimeException();
        }
    }


    /**
     * print a tuple
     **/
    public static void PPrint(Tuple t) {
        for (int i = 0; i < t.getData().size(); i++) {
            Object data = t.getData(i);
            if (data instanceof Integer) {
                System.out.print((Integer) data + "\t");
            } else if (data instanceof Float) {
                System.out.print((Float) data + "\t");
            } else {
                System.out.print(((String) data) + "\t");
            }
        }
        System.out.println();
    }


    /**
     * print a page of tuples
     **/
    public static void PPrint(Batch b) {
        for (int i = 0; i < b.size(); i++) {
            PPrint(b.getRecord(i));
            System.out.println();
        }
    }

}













