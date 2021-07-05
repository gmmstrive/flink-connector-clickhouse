# Apache Ignite connector fo Flink SQL
This connector allows reading data from Ignite tables using JDBC thin client.

## Dependencies
**flink-connector-ignite** expects `ignite-core` jar on the classpath in version allowing to connect to your Ignite cluster.

## Source table DDL
```sql
CREATE TABLE ignite_source (
    id INT NOT NULL,
    name STRING,
    weight DECIMAL(10,2)
) WITH (
    'connector' = 'ignite',
    'url' = 'jdbc:ignite:thin://127.0.0.1',
    'username' = 'ignite',
    'password' = 'ignite',
    'table-name' = 'test'
);
```

## Partitioning
Ignite connector allows you to specify date ranges which are treated as partitions - data
for each day is fetched using separate query. This is useful if you have Ignite data collocated by
date.

```sql
CREATE TABLE ignite_source (
    id INT NOT NULL,
    name STRING,
    dat_date TIMESTAMP
    weight DECIMAL(10,2)
) WITH (
    'connector' = 'ignite',
    'url' = 'jdbc:ignite:thin://127.0.0.1',
    'username' = 'ignite',
    'password' = 'ignite',
    'table-name' = 'test',
    'scan.partition.column' = 'day_date', 
    'scan.partition.lower-bound' = '2021-05-01', 
    'scan.partition.upper-bound' = '2021-05-31'
);
```

### Licence
**flink-connector-ignite** is published under Apache License 2.0.
