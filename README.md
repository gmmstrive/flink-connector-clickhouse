# flink-connector-clickhouse

#### flink sql 自定义 connector

```sql
%flink.conf

flink.yarn.appName zeppelin-test-ch

flink.execution.jars /Users/lucas/IdeaProjects/microi/flink-microi-conn/clickhouse/target/clickhouse-1.0-SNAPSHOT.jar
```

```sql
%flink.ssql

DROP TABLE IF EXISTS ch;
CREATE TABLE ch (
    id String,
    name String,
    age String,
    create_date Date
) WITH (
    'connector' = 'clickhouse',
    'url' = 'jdbc:clickhouse://172.16.8.164:8123/temp',
    'table-name' = 'user_name',
    'username' = 'default',
    'password' = '',
    'format' = 'json'
);
```

```sql
%flink.ssql(type=update)

select * from ch;

```

```sql
%flink.ssql(type=update)

insert into ch 
select '8' as id, '3' as name, '4' as age, cast('2020-01-03' as DATE) as create_date

```
