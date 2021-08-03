package pl.touk.flink.ignite.ddl;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;

public class IgniteSourceTableDDLBuilder {

    private String tableName;
    private List<String> columnsDefinition;
    private String igniteUrl;
    private String igniteTableName;
    private LocalDate partitionLowerBound;
    private LocalDate partitionUpperBound;
    private String partitionColumn;
    private ZoneId timezone;
    private String username;
    private String password;

    public IgniteSourceTableDDLBuilder withTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public IgniteSourceTableDDLBuilder withColumnsDefinition(List<String> columnsDefinition) {
        this.columnsDefinition = columnsDefinition;
        return this;
    }

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

    public IgniteSourceTableDDLBuilder withUsername(String username) {
        this.username = username;
        return this;
    }

    public IgniteSourceTableDDLBuilder withPassword(String password) {
        this.password = password;
        return this;
    }

    public String build() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("CREATE TABLE %s (", tableName))
                .append(String.join(",\n", columnsDefinition))
                .append(String.format(") WITH ("
                                + " 'connector' = 'ignite',"
                                + " 'url' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'table-name' = '%s'",
                        igniteUrl, username, password, igniteTableName));

        if (partitionColumn != null && partitionLowerBound != null && partitionUpperBound != null && timezone != null) {
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
