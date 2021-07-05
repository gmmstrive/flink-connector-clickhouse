package pl.touk.flink.ignite.table;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Objects;

public class JdbcDatePartitionReadOptions {

    private final String partitionColumnName;
    private final LocalDate partitionLowerBound;
    private final LocalDate partitionUpperBound;
    private final ZoneId timezone;

    private JdbcDatePartitionReadOptions(String partitionColumnName, LocalDate partitionLowerBound, LocalDate partitionUpperBound, ZoneId timezone) {
        this.partitionColumnName = partitionColumnName;
        this.partitionLowerBound = partitionLowerBound;
        this.partitionUpperBound = partitionUpperBound;
        this.timezone = timezone;
    }

    public String getPartitionColumnName() {
        return partitionColumnName;
    }

    public LocalDate getPartitionLowerBound() {
        return partitionLowerBound;
    }

    public LocalDate getPartitionUpperBound() {
        return partitionUpperBound;
    }

    public ZoneId getTimezone() {
        return timezone;
    }

    public static JdbcDatePartitionReadOptions.Builder builder() {
        return new JdbcDatePartitionReadOptions.Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof JdbcDatePartitionReadOptions) {
            JdbcDatePartitionReadOptions options = (JdbcDatePartitionReadOptions) o;
            return  Objects.equals(partitionColumnName, options.partitionColumnName) &&
                    Objects.equals(partitionLowerBound, options.partitionLowerBound) &&
                    Objects.equals(partitionUpperBound, options.partitionUpperBound) &&
                    Objects.equals(timezone, options.timezone);
        } else {
            return false;
        }
    }

    public static class Builder {
        protected String partitionColumnName;
        protected LocalDate partitionLowerBound;
        protected LocalDate partitionUpperBound;
        protected ZoneId timezone;

        public JdbcDatePartitionReadOptions.Builder setPartitionColumnName(String partitionColumnName) {
            this.partitionColumnName = partitionColumnName;
            return this;
        }

        public JdbcDatePartitionReadOptions.Builder setPartitionLowerBound(String partitionLowerBound) {
            this.partitionLowerBound = LocalDate.parse(partitionLowerBound);
            return this;
        }

        public JdbcDatePartitionReadOptions.Builder setPartitionUpperBound(String partitionUpperBound) {
            this.partitionUpperBound = LocalDate.parse(partitionUpperBound);
            return this;
        }

        public JdbcDatePartitionReadOptions.Builder setTimezone(ZoneId timezone) {
            this.timezone = timezone;
            return this;
        }

        public JdbcDatePartitionReadOptions build() {
            return new JdbcDatePartitionReadOptions(partitionColumnName, partitionLowerBound, partitionUpperBound, timezone);
        }
    }
}
