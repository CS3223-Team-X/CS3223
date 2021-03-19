import qp.operators.Debug;
import qp.operators.Operator;
import qp.optimizer.BufferManager;
import qp.optimizer.PlanCost;
import qp.optimizer.RandomOptimizer;
import qp.parser.Scaner;
import qp.parser.parser;
import qp.utils.*;

import java.io.*;
import java.util.List;

/**
 * The driver program of the query processor.
 */
public class QueryMain {

    static PrintWriter out;
    static int numAtts;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("usage: java QueryMain <queryfilename> <resultfile> <pagesize> <numbuffer>");
            System.exit(1);
        }

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        Batch.setPageSize(getPageSize(args, in));

        SQLQuery sqlquery = getSQLQuery(args[0]);
        configureBufferManager(sqlquery.getNumJoin(), args, in);

        verifySufficientPageSize(sqlquery.getFromList(), Batch.getPageSize());

        Operator root = getQueryPlan(sqlquery);
        printFinalPlan(root, args, in);
        executeQuery(root, args[1]);
    }

    /**
     * Get page size from arguments, if not provided request as input
     **/
    private static int getPageSize(String[] args, BufferedReader in) {
        int pagesize = -1;
        if (args.length < 3) {
            System.out.println("enter the number of bytes per page");
            try {
                String temp = in.readLine();
                pagesize = Integer.parseInt(temp);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else pagesize = Integer.parseInt(args[2]);
        return pagesize;
    }

    /**
     * Parse query from query file
     **/
    public static SQLQuery getSQLQuery(String queryfile) {
        /** Read query file **/
        FileInputStream source = null;
        try {
            source = new FileInputStream(queryfile);
        } catch (FileNotFoundException ff) {
            System.out.println("File not found: " + queryfile);
            System.exit(1);
        }

        /** Scan the query **/
        Scaner sc = new Scaner(source);
        parser p = new parser();
        p.setScanner(sc);

        /** Parse the query **/
        try {
            p.parse();
        } catch (Exception e) {
            System.out.println("Exception occured while parsing");
            System.exit(1);
        }

        return p.getSQLQuery();
    }

    /**
     * If there are joins then assigns buffers to each join operator while preparing the plan.
     * As buffer manager is not implemented, just input the number of buffers available.
     **/
    private static void configureBufferManager(int numJoin, String[] args, BufferedReader in) {
        int numBuff = 1000;
        if (args.length < 4) {
            System.out.println("enter the number of buffers available");
            try {
                String temp = in.readLine();
                numBuff = Integer.parseInt(temp);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else numBuff = Integer.parseInt(args[3]);
        BufferManager.init(numBuff, numJoin);

        /** Check the number of buffers available is enough or not **/
        if (numJoin > 0) {
            int buffersPerJoin = BufferManager.getBuffersPerJoin();
            if (buffersPerJoin < 3) {
                System.out.println("Minimum 3 buffers are required per join operator ");
                System.exit(1);
            }
        }
    }

    private static void verifySufficientPageSize(List<String> tables, int pageSize) {
        int cumulativeRecordSize = 0;
        for (String table : tables) {
            try (BufferedReader file = new BufferedReader(new FileReader(table + ".det"))){
                file.readLine();
                int recordSize = Integer.parseInt(file.readLine().trim());
                cumulativeRecordSize += recordSize;
            } catch(IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        if (cumulativeRecordSize > pageSize) {
            System.out.println("Page size of " + pageSize + " is insufficient for cumulative record size of " + cumulativeRecordSize);
            System.exit(1);
        } else {
            System.out.println("Page size of " + pageSize + " and cumulative record size of " + cumulativeRecordSize);
        }
    }

    /**
     * Run optimiser and get the final query plan as an Operator
     **/
    public static Operator getQueryPlan(SQLQuery sqlquery) {
        Operator root = null;

        RandomOptimizer optimizer = new RandomOptimizer(sqlquery);
        Operator planroot = optimizer.getOptimizedPlan();

        if (planroot == null) {
            System.out.println("DPOptimizer: query plan is null");
            System.exit(1);
        }

        root = RandomOptimizer.makeExecPlan(planroot);

        return root;
    }

    /**
     * Print final Plan and ask user whether to continue
     **/
    private static void printFinalPlan(Operator root, String[] args, BufferedReader in) {
        System.out.println("----------------------Execution Plan----------------");
        Debug.PPrint(root);
        PlanCost pc = new PlanCost();
        System.out.printf("\nExpected cost: %d\n", pc.getCost(root));
        if (args.length < 5) {
            /** Ask user whether to continue execution of the program **/
            System.out.println("enter 1 to continue, 0 to abort ");
            try {
                String temp = in.readLine();
                int flag = Integer.parseInt(temp);
                if (flag == 0) {
                    System.exit(1);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Execute query and print run statistics
     **/
    public static double executeQuery(Operator root, String resultfile) {
        long startTime = System.currentTimeMillis();
        if (!root.open()) {
            System.out.println("Root: Error in opening of root");
            System.exit(1);
        }
        try {
            out = new PrintWriter(new BufferedWriter(new FileWriter(resultfile)));
        } catch (IOException io) {
            System.out.println("QueryMain:error in opening result file: " + resultfile);
            System.exit(1);
        }

        /** Print the schema of the result **/
        Schema schema = root.getSchema();
        numAtts = schema.getNumCols();
        printSchema(schema);

        /** Print each tuple in the result **/
        Batch resultbatch;
        while ((resultbatch = root.next()) != null) {
            for (int i = 0; i < resultbatch.size(); ++i) {
                printTuple(resultbatch.getRecord(i));
            }
        }
        root.close();
        out.close();

        long endTime = System.currentTimeMillis();
        double executionTime = (endTime - startTime) / 1000.0;
        System.out.println("Execution time = " + executionTime);
        return executionTime;
    }

    protected static void printSchema(Schema schema) {
        String[] aggregates = new String[]{"", "MAX", "MIN", "SUM", "COUNT", "AVG"};
        for (int i = 0; i < numAtts; ++i) {
            Attribute attr = schema.getAttribute(i);
            int aggregate = attr.getAggType();
            String tabname = attr.getTabName();
            String colname = attr.getColName();
            if (aggregate == 0) {
                out.print(tabname + "." + colname + "  ");
            } else {
                out.print(aggregates[aggregate] + "(" + tabname + "." + colname + ")  ");
            }
        }
        out.println();
    }

    protected static void printTuple(Tuple t) {
        for (int i = 0; i < numAtts; ++i) {
            Object data = t.getData(i);
            if (data instanceof Integer) {
                out.print(((Integer) data).intValue() + "\t");
            } else if (data instanceof Float) {
                out.print(((Float) data).floatValue() + "\t");
            } else if (data == null) {
                out.print("-NULL-\t");
            } else {
                out.print(((String) data) + "\t");
            }
        }
        out.println();
    }
}