# CS3223 Database System Implementation Project
**AY20-21 Semester 2**  
**National University of Singapore, School of Computing**

## About
This is our implementation for a project in the module CS3223 Database Systems Implementation.
Our team consists of the following students from National University of Singapore, School of Computing:
* [YIK REN JUN, GABRIEL](https://github.com/GabrielYik)
* [KANG WANGKAI](https://github.com/Kangwkk)
* [JOVAN HUANG TIAN CHUN](https://github.com/jovanhuang)

The project aimed to let us gain a feel for how query processing works through development and enhancement of a simple Select-Project-Join (SPJ) query engine.  

We saw how different query execution trees have different performance results, which provides motivation for query optimization.  

This query engine provides rudimentary functionality and ignores many of the complex aspects of a real system.  

More information about this project can be found [here](https://www.comp.nus.edu.sg/~tankl/cs3223/project.html).

## Setup
### Prerequisites
* Java version 11.

### Environment Setup
#### Windows
1. Run the script `queryenv.bat` to set up environment variables
2. Run the script `build.bat` to compile required files

#### Unix
1. Run the script `queryenv` (command `source queryenv`) to set up environment variables
2. Run the script `build.sh` to compile required files

### Tables Setup
1. Run the helper program `RandomDB` (command `java RandomDB <table-name> <number-of-records-in-table>`) for a table to generate preliminary files (`<table-name>`.md, `<table-name>`.stat, `<table-name>`.txt)
2. Run the helper program `ConvertTxtToTbl` (command `java ConvertTxtToTbl <table-name>`) for a table to generate a file containing a table, with records stored as objects, from the `<table>.txt` file

### Running
* Run the main program `QueryMain` (command `java QueryMain <query-in-file> <query-out-file> [page-size] [page-count]`)

## Chosen Implementations
1. Block Nested Loop Join (see: [BlockNestedJoin.java](src/qp/operators/joins/BlockNestedJoin.java))
2. Sort-Merge Join (see: [SortMergeJoin.java](src/qp/operators/joins/SortMergeJoin.java))
3. DISTINCT (see: [Distinct.java](src/qp/operators/Distinct.java))
4. ORDERBY (see: [OrderBy.java](src/qp/operators/OrderBy.java))
5. Aggregate functions: MIN, MAX, COUNT, AVG (see: [aggregates](src/qp/operators/projects/aggregates))

## Additional Implementations
1. SUM (an aggregate function)

## Implementation Notes
### Joins
* The stock Page Nested Loop Join is purposed as a generic Nested Loop Join with a variable input buffer size
* Both Block Nested Loop Join and Page Nested Loop Join (now separate from the stock version) utilise Nested Loop Join with different input buffer sizes;
  default of 1 for Page Nested Join, and a variable number for Block Nested Join
### Sorting
* The External Sort-Merge algorithm is implemented and utilised by a few implementations: Sort-Merge Join, DISTINCT and ORDERBY
### Aggregates
* Unlike the other implementations, which are modeled as nodes in a query plan tree, the aggregate functions do not fit such a model, and are used only during attribute projection

## Fixes
* Disallow further program execution when the value of `[page-size]` in the running of the main program `QueryMain` is less than the cumulative size of all specified tuple sizes

## Further References
* [Original User Guide](https://www.comp.nus.edu.sg/~tankl/cs3223/project/user.htm)
* [Original Developer Guide](https://www.comp.nus.edu.sg/~tankl/cs3223/project/developer.htm)
