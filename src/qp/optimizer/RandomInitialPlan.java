package qp.optimizer;

import qp.operators.*;
import qp.operators.joins.Join;
import qp.operators.joins.JoinType;
import qp.operators.projects.Project;
import qp.utils.*;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.*;

/**
 * A generator of a random initial plan for a query.
 */
class RandomInitialPlan {
    private final SQLQuery sqlquery;

    private final List<Attribute> projectedAttributes;
    private final List<String> fromTables;
    private final List<Condition> selectionConditions;
    private final List<Condition> joinConditions;
    private final List<Attribute> orderByAttributes;

    /**
     * Contains mappings between a table to its corresponding
     * {@code Scan} operator.
     */
    private final Map<String, Operator> tableToOperator;

    private Operator queryPlanRoot;

    public RandomInitialPlan(SQLQuery sqlQuery) {
        this.sqlquery = sqlQuery;

        projectedAttributes = sqlQuery.getProjectList();
        fromTables = sqlQuery.getFromList();
        selectionConditions = sqlQuery.getSelectionList();
        joinConditions = sqlQuery.getJoinList();
        orderByAttributes = sqlQuery.getOrderByList();

        tableToOperator = new HashMap<>();
    }

    /**
     * Prepare an initial plan for the query
     **/
    public Operator prepareInitialPlan() {
        if (sqlquery.isDistinct()) {
            System.err.println("Distinct is not implemented.");
            System.exit(1);
        }

        if (sqlquery.getGroupByList().size() > 0) {
            System.err.println("GroupBy is not implemented.");
            System.exit(1);
        }

        createScanOperators();
        createSelectOperators();
        createJoinOperators();
        createProjectOperators();
        createOrderByOperators();

        return queryPlanRoot;
    }

    /**
     * Creates a {@code Scan} operator for each table specified
     * in the FROM clause.
     */
    public void createScanOperators() {
        if (fromTables.isEmpty()) {
            // should not reach here
            throw new RuntimeException();
        }

        // will never remain null
        Scan lastScan = null;
        for (String fromTable : fromTables) {
            Scan scan = new Scan(fromTable);
            lastScan = scan;

            String tableMetadata = fromTable + ".md";
            try {
                ObjectInputStream in = new ObjectInputStream(new FileInputStream(tableMetadata));
                Schema schema = (Schema) in.readObject();
                scan.setSchema(schema);
                in.close();
            } catch (Exception e) {
                System.err.println("RandomInitialPlan:Error reading Schema of the table " + tableMetadata);
                e.printStackTrace();
                System.exit(1);
            }
            tableToOperator.put(fromTable, scan);
        }

        // for the case where where all attributes are to be selected i.e., Select *
        if (selectionConditions.isEmpty()) {
            queryPlanRoot = lastScan;
        }
    }

    /**
     * Creates a {@code Selection} operator for each condition,
     * between an attribute and a string, specified in the WHERE clause.
     */
    public void createSelectOperators() {
        if (selectionConditions.isEmpty()) {
            return;
        }

        // will never remain null
        Select lastSelect = null;
        for (Condition selectionCondition : selectionConditions) {
            if (selectionCondition.getOpType() == Condition.SELECT) {
                String table = selectionCondition.getLhs().getTabName();
                Operator operatorOfTable = tableToOperator.get(table);
                lastSelect = new Select(operatorOfTable, selectionCondition);
                lastSelect.setSchema(operatorOfTable.getSchema());
                modifyHashtable(operatorOfTable, lastSelect);
            }
        }
        queryPlanRoot = lastSelect;
    }

    /**
     * Creates a {@code Join} operator for each condition,
     * between two attributes, specified in the WHERE clause.
     */
    public void createJoinOperators() {
        int numJoins = joinConditions.size();
        if (numJoins == 0) {
            return;
        }

        BitSet joinConditionIndices = new BitSet(numJoins);
        int joinConditionIndex = RandomNumberGenerator.randInt(0, numJoins - 1);
        // will never remain null
        Join join = null;

        // repeat until all join conditions are considered
        while (joinConditionIndices.cardinality() != numJoins) {
            // choose an unconsidered join condition at random
            while (joinConditionIndices.get(joinConditionIndex)) {
                joinConditionIndex = RandomNumberGenerator.randInt(0, numJoins - 1);
            }
            Condition joinCondition = joinConditions.get(joinConditionIndex);
            String leftTable = joinCondition.getLhs().getTabName();
            String rightTable = ((Attribute) joinCondition.getRhs()).getTabName();
            Operator leftTableScan = tableToOperator.get(leftTable);
            Operator rightTableScan = tableToOperator.get(rightTable);
            join = new Join(leftTableScan, rightTableScan, joinCondition);
            join.setNodeIndex(joinConditionIndex);
            Schema joinedSchema = leftTableScan.getSchema().joinWith(rightTableScan.getSchema());
            join.setSchema(joinedSchema);

            // choose a join type at random
            int numJoinTypes = JoinType.numJoinTypes();
            int joinType = RandomNumberGenerator.randInt(0, numJoinTypes - 1);
            join.setJoinType(joinType);

            modifyHashtable(leftTableScan, join);
            modifyHashtable(rightTableScan, join);

            joinConditionIndices.set(joinConditionIndex);
        }

        queryPlanRoot = join;
    }

    /**
     * Creates a {@code Project} operator for each attribute
     * specified in the SELECT clause.
     */
    public void createProjectOperators() {
        if (projectedAttributes.isEmpty()) {
            return;
        }

        Schema projectedSchema = queryPlanRoot.getSchema().subSchema(projectedAttributes);
        Project project = new Project(queryPlanRoot, projectedAttributes);
        project.setSchema(projectedSchema);

        queryPlanRoot = project;
    }

    /**
     * Creates a {@code OrderBy} operator for each attribute
     * specified in the ORDERBY clause.
     */
    private void createOrderByOperators() {
        if (orderByAttributes.isEmpty()) {
            return;
        }

        Sort.Direction sortDirection = sqlquery.isDesc() ? Sort.Direction.DSC : Sort.Direction.ASC;

        OrderBy orderBy = new OrderBy(queryPlanRoot, orderByAttributes, sortDirection);
        orderBy.setSchema(queryPlanRoot.getSchema());

        queryPlanRoot = orderBy;
    }

    private void modifyHashtable(Operator oldOperator, Operator newOperator) {
        for (Map.Entry<String, Operator> entry : tableToOperator.entrySet()) {
            if (entry.getValue().equals(oldOperator)) {
                entry.setValue(newOperator);
            }
        }
    }
}
