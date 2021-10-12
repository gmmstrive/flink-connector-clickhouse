package pl.touk.flink.ignite.table;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import pl.touk.flink.ignite.dialect.IgniteDialect;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class IgniteDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "ignite";

    private static final String DRIVER_NAME = "org.apache.ignite.IgniteJdbcThinDriver";

    public static final ConfigOption<String> URL = ConfigOptions
            .key("url")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc database url.");

    public static final ConfigOption<String> TABLE_NAME = ConfigOptions
            .key("table-name")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc table name.");

    public static final ConfigOption<String> USERNAME = ConfigOptions
            .key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc user name.");

    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("the jdbc password.");

    private static final ConfigOption<String> SCAN_PARTITION_COLUMN = ConfigOptions
            .key("scan.partition.column")
            .stringType()
            .noDefaultValue()
            .withDescription("the column name used for partitioning the input.");

    private static final ConfigOption<String> SCAN_PARTITION_TIMEZONE = ConfigOptions
            .key("scan.partition.timezone")
            .stringType()
            .noDefaultValue()
            .withDescription("the timezone of data in column name used for partitioning the input.");

    private static final ConfigOption<String> SCAN_PARTITION_LOWER_BOUND = ConfigOptions
            .key("scan.partition.lower-bound")
            .stringType()
            .noDefaultValue()
            .withDescription("day of the first partition.");

    private static final ConfigOption<String> SCAN_PARTITION_UPPER_BOUND = ConfigOptions
            .key("scan.partition.upper-bound")
            .stringType()
            .noDefaultValue()
            .withDescription("day of the last partition.");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(TABLE_NAME);
        requiredOptions.add(USERNAME);
        requiredOptions.add(PASSWORD);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(SCAN_PARTITION_COLUMN);
        optionalOptions.add(SCAN_PARTITION_LOWER_BOUND);
        optionalOptions.add(SCAN_PARTITION_UPPER_BOUND);
        optionalOptions.add(SCAN_PARTITION_TIMEZONE);
        return optionalOptions;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        // either implement your custom validation logic here ...
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        final ReadableConfig config = helper.getOptions();

        // validate all options
        helper.validate();

        JdbcConnectorOptions jdbcOptions = getJdbcOptions(config);
        JdbcDatePartitionReadOptions readOptions = getJdbcReadOptions(config).orElse(null);

        // table source
        return new IgniteDynamicTableSource(jdbcOptions, readOptions, context.getCatalogTable().getResolvedSchema());

    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        throw new NotImplementedException("Ignite dynamic sink not implemented yet");
    }

    private JdbcConnectorOptions getJdbcOptions(ReadableConfig readableConfig) {
        final String url = readableConfig.get(URL);
        final JdbcConnectorOptions.Builder builder = JdbcConnectorOptions.builder()
                .setDriverName(DRIVER_NAME)
                .setDBUrl(url)
                .setTableName(readableConfig.get(TABLE_NAME))
                .setDialect(new IgniteDialect());

        readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
        readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
        return builder.build();
    }

    private Optional<JdbcDatePartitionReadOptions> getJdbcReadOptions(ReadableConfig readableConfig) {
        final Optional<String> partitionColumnName = readableConfig.getOptional(SCAN_PARTITION_COLUMN);
        return partitionColumnName.map(columnName ->
            JdbcDatePartitionReadOptions.builder()
                    .setPartitionColumnName(partitionColumnName.get())
                        .setTimezone(ZoneId.of(readableConfig.get(SCAN_PARTITION_TIMEZONE)))
                        .setPartitionLowerBound(readableConfig.get(SCAN_PARTITION_LOWER_BOUND))
                        .setPartitionUpperBound(readableConfig.get(SCAN_PARTITION_UPPER_BOUND))
                    .build()
        );
    }

}
