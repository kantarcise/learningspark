# Join Strategies ? What is that mean? 🤔

Join strategies in Spark refer to the different methods or algorithms used by the Spark engine to physically execute the join operation on distributed data. These strategies determine how data is moved, partitioned, and combined across the executors to perform the join. They are concerned with the "how" of the join operation, focusing on the mechanics of data movement and execution efficiency.

On the other hand, join types (like inner join, left outer join, etc.) specify the "what" of the join operation, focusing on the logical rules that determine which rows from the tables should be included in the result based on the join condition.

Spark has five distinct join strategies by which it exchanges, moves, sorts, groups, and merges data across executors: the broadcast hash join (BHJ), shuffle hash join (SHJ), shuffle sort merge join (SMJ), broadcast nested loop join (BNLJ), and shuffle-and replicated nested loop join (a.k.a. Cartesian product join).

## Join Types (Logical Level) 🤽

### Inner Join: 

Includes only the rows that have matching keys in both tables.

### Left Outer Join: 

Includes all rows from the left table and the matching rows from the right table. Non-matching rows from the right table result in NULLs.

### Right Outer Join: 

Includes all rows from the right table and the matching rows from the left table. Non-matching rows from the left table result in NULLs.

### Full Outer Join: 

Includes rows when there is a match in one of the tables. Rows without a match in the other table result in NULLs.

### Cross Join (Cartesian Product): 

Every row from the left table is combined with every row from the right table.

## Join Strategies (Physical Level) 📌

### Broadcast Hash Join (BHJ):

How: Broadcasts the smaller table to all executors. Each executor then performs a hash join with the larger table.
Use Case: Efficient for joining a small table with a large table.

### Shuffle Hash Join (SHJ):

How: Partitions both tables by the join key and performs a hash join on the shuffled data.
Use Case: Suitable for large tables where broadcasting isn't feasible.

### Shuffle Sort Merge Join (SMJ):

How: Shuffles and sorts both tables by the join key before merging them.
Use Case: Efficient for large tables that are already sorted or can be sorted efficiently.

### Broadcast Nested Loop Join (BNLJ):

How: Broadcasts the smaller table and performs a nested loop join with the larger table.
Use Case: Used when join conditions are not equi-joins or for joins without conditions.

### Shuffle-and-Replicated Nested Loop Join (Cartesian Product Join):

How: Shuffles and replicates data to perform a Cartesian product.
Use Case: For combinatorial analysis requiring Cartesian products.

## Here are more details about the 5 join Strategies of Spark.

### 1. Broadcast Hash Join (BHJ)

#### Description: 

Broadcast Hash Join is used when one of the tables is small enough to fit into memory. Spark broadcasts the small table to all executors and performs a hash join with the larger table.

#### Use Case Example:

Scenario: You have a large table of sales transactions and a small table of product details.
Example: Join the sales transactions with product details to enrich the transactions with product information.

```python
small_df = spark.read.csv("small_product_details.csv")
large_df = spark.read.csv("large_sales_transactions.csv")

joined_df = large_df.join(broadcast(small_df), "product_id")
```

### 2. Shuffle Hash Join (SHJ)

#### Description: 

Shuffle Hash Join is used when both tables are too large to fit into memory. Spark performs a hash join by shuffling data so that rows with the same join key end up on the same executor.

#### Use Case Example:

Scenario: You have two large tables, one with user information and one with user purchase history.
Example: Join the user information with their purchase history.

```python
large_df1 = spark.read.csv("large_user_info.csv")
large_df2 = spark.read.csv("large_purchase_history.csv")

joined_df = large_df1.join(large_df2, "user_id")
```

### 3. Shuffle Sort Merge Join (SMJ)

#### Description: 

Shuffle Sort Merge Join is efficient for large tables with sorted data. It shuffles and sorts both tables based on the join keys before merging them.

#### Use Case Example:

Scenario: You have two large sorted tables, one with employee records and another with department records.
Example: Join the employee records with department records.

```python
large_df1 = spark.read.csv("sorted_employee_records.csv")
large_df2 = spark.read.csv("sorted_department_records.csv")

joined_df = large_df1.join(large_df2, "department_id")
```

### 4. Broadcast Nested Loop Join (BNLJ)

#### Description: 

Broadcast Nested Loop Join is used when there is no join condition or when join conditions are not equi-joins (non-equality conditions). Spark broadcasts the smaller table and performs a nested loop join.

#### Use Case Example:

Scenario: You need to find all combinations of rows from two small tables.
Example: Cartesian product of two small tables of products and stores.

```python
small_df1 = spark.read.csv("small_products.csv")
small_df2 = spark.read.csv("small_stores.csv")

cross_joined_df = small_df1.crossJoin(small_df2)
```

### 5. Shuffle-and-Replicated Nested Loop Join (Cartesian Product Join)

#### Description: 

This join strategy is used for Cartesian products where every row from one table is joined with every row from another table. It involves shuffling and replicating data across executors.

#### Use Case Example:

Scenario: You need to compute all possible combinations between rows from two large tables.
Example: Cartesian product of two large tables for a combinatorial analysis.

```python
large_df1 = spark.read.csv("large_table1.csv")
large_df2 = spark.read.csv("large_table2.csv")

cross_joined_df = large_df1.crossJoin(large_df2)
```

### Summary

- BHJ is ideal for joining a large table with a small table.
- SHJ is used for large tables where data needs to be shuffled and hashed.
- SMJ is optimal for large, sorted tables.
- BNLJ handles non-equi joins and joins without conditions.
- Cartesian Product Join computes Cartesian products for combinatorial use cases.

By selecting the appropriate join strategy, Spark optimizes performance and resource utilization based on the characteristics of the datasets involved.