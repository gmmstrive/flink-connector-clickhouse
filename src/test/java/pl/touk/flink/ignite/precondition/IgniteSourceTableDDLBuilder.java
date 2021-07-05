package pl.touk.flink.ignite.precondition;

import java.time.LocalDate;
import java.time.ZoneId;

public class IgniteSourceTableDDLBuilder {

    private String igniteUrl;
    private String igniteTableName;
    private LocalDate partitionLowerBound;
    private LocalDate partitionUpperBound;
    private String partitionColumn;
    private ZoneId timezone;

    public static final String tableName = "ignite_source";

    public IgniteSourceTableDDLBuilder withIgniteUrl(String igniteUrl) {
        this.igniteUrl = igniteUrl;
        return this;
    }

    public IgniteSourceTableDDLBuilder withIgniteTableName(String igniteTableName) {
        this.igniteTableName = igniteTableName;
        return this;
    }

    public IgniteSourceTableDDLBuilder withPartitionLowerBound(LocalDate partitionLowerBound) {
        this.partitionLowerBound = partitionLowerBound;
        return this;
    }

    public IgniteSourceTableDDLBuilder withPartitionUpperBound(LocalDate partitionUpperBound) {
        this.partitionUpperBound = partitionUpperBound;
        return this;
    }

    public IgniteSourceTableDDLBuilder withPartitionColumn(String partitionColumn) {
        this.partitionColumn = partitionColumn;
        return this;
    }

    public IgniteSourceTableDDLBuilder withTimezone(ZoneId timezone) {
        this.timezone = timezone;
        return this;
    }

    public String build() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("CREATE TABLE %s ("
                    + " id INT NOT NULL,"
                    + " name STRING,"
                    + " weight DECIMAL(10,2)"
                    + ") WITH ("
                    + " 'connector' = 'ignite',"
                    + " 'url' = '%s',"
                    + " 'username' = 'ignite',"
                    + " 'password' = 'ignite',"
                    + " 'table-name' = '%s'",
                tableName, igniteUrl, igniteTableName));

        if (partitionLowerBound != null && partitionUpperBound != null && timezone != null) {
            sb.append(String.format(", 'scan.partition.lower-bound' = '%s',"
                            + " 'scan.partition.upper-bound' = '%s',"
                            + " 'scan.partition.column' = '%s',"
                            + " 'scan.partition.timezone' = '%s'",
                    partitionLowerBound, partitionUpperBound, partitionColumn, timezone)
            );
        }
        sb.append(")");
        return sb.toString();
    }

}
