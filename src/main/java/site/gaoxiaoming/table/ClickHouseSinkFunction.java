package site.gaoxiaoming.table;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.domain.ClickHouseFormat;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.io.ByteArrayInputStream;
import java.sql.SQLException;

/**
 * @author lucas
 */
public class ClickHouseSinkFunction extends RichSinkFunction<RowData> {

    private static final long serialVersionUID = 1L;

    private static final String MAX_PARALLEL_REPLICAS_VALUE = "2";

    private final JdbcOptions jdbcOptions;
    private final SerializationSchema<RowData> serializationSchema;

    private ClickHouseConnection conn;
    private ClickHouseStatement stmt;


    public ClickHouseSinkFunction(JdbcOptions jdbcOptions, SerializationSchema<RowData> serializationSchema) {
        this.jdbcOptions = jdbcOptions;
        this.serializationSchema = serializationSchema;
    }

    @Override
    public void open(Configuration parameters) {
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(jdbcOptions.getUsername().orElse(null));
        properties.setPassword(jdbcOptions.getPassword().orElse(null));
        BalancedClickhouseDataSource dataSource;
        try {
            if (null == conn) {
                dataSource = new BalancedClickhouseDataSource(jdbcOptions.getDbURL(), properties);
                conn = dataSource.getConnection();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        byte[] serialize = serializationSchema.serialize(value);
        stmt = conn.createStatement();
        stmt.write().table(jdbcOptions.getTableName()).data(new ByteArrayInputStream(serialize), ClickHouseFormat.JSONEachRow)
                .addDbParam(ClickHouseQueryParam.MAX_PARALLEL_REPLICAS, MAX_PARALLEL_REPLICAS_VALUE).send();
    }

    @Override
    public void close() throws Exception {
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

}
