package pl.touk.flink.ignite;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import pl.touk.flink.ignite.ddl.IgniteSourceTableDDLBuilder;
import pl.touk.flink.ignite.precondition.IgniteJdbcClient;
import pl.touk.flink.ignite.precondition.Preconditions;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

public class FlinkConnectorIgniteIT {

    private static final String tableName = "ignite_source";
    private static int ignitePort;
    private static Ignite ignite;
    private static StreamTableEnvironment tableEnv;

    private static final Preconditions given = new Preconditions();

    @BeforeAll
    static void setUp() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        tableEnv = StreamTableEnvironmentUtil.create(
                env, EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
        );

        ignitePort = PortFinder.getAvailablePort();
        File igniteWorkDir = Files.createTempDirectory("igniteSpec").toFile();
        igniteWorkDir.deleteOnExit();
        ignite = Ignition.start(new IgniteConfiguration()
                .setWorkDirectory(igniteWorkDir.getAbsolutePath())
                .setClientConnectorConfiguration(
                        new ClientConnectorConfiguration()
                                .setPort(ignitePort).setPortRange(10)
                )
        );

        given.igniteClient(ignitePort).schemaCreated();
    }

    @AfterAll
    static void afterAll() {
        ignite.close();
    }

    @AfterEach
    void afterEach() {
        tableEnv.executeSql("DROP TABLE " + tableName);
    }

    @Test
    void shouldRetrieveAllData() throws Exception {
        // given
        Timestamp day = Timestamp.valueOf("2021-06-24 00:00:00");
        IgniteJdbcClient.TestRecord[] records = new IgniteJdbcClient.TestRecord[]{
                new IgniteJdbcClient.TestRecord(1, "alpha", BigDecimal.valueOf(10), day),
                new IgniteJdbcClient.TestRecord(2, "alpha", BigDecimal.valueOf(11), day),
                new IgniteJdbcClient.TestRecord(3, "bravo", BigDecimal.valueOf(5), day),
        };

        given.igniteClient(ignitePort)
                .testData(records);

        String sourceDDL = igniteTableBuilder().build();

        tableEnv.executeSql(sourceDDL);

        // when
        TableResult result =
                tableEnv.executeSql(
                        "SELECT name, SUM(weight) FROM ignite_source GROUP BY name");

        // then
        CloseableIterator<Row> collect = result.collect();
        List<Row> results = new ArrayList<>();
        collect.forEachRemaining(results::add);
        assertThat(results).hasSize(2)
                .extracting(row -> tuple(row.getField(0), row.getField(1)))
                .containsExactlyInAnyOrder(
                        tuple("alpha", bigDecimal(21)),
                        tuple("bravo", bigDecimal(5))
                );
    }

    @Test
    void shouldRetrieveUsingPartitions() throws Exception {
        // given
        LocalDate start = LocalDate.parse("2021-06-01");
        LocalDate rangeStart = start.plusDays(1);
        LocalDate rangeEnd = start.plusDays(3);
        IgniteJdbcClient.TestRecord[] records = new IgniteJdbcClient.TestRecord[] {
                new IgniteJdbcClient.TestRecord(1, "alpha", BigDecimal.valueOf(10), timestamp(start)),
                new IgniteJdbcClient.TestRecord(2, "bravo", BigDecimal.valueOf(11), timestamp(rangeStart)),
                new IgniteJdbcClient.TestRecord(3, "charlie", BigDecimal.valueOf(5), timestamp(start.plusDays(2))),
                new IgniteJdbcClient.TestRecord(4, "charlie", BigDecimal.valueOf(7), timestamp(rangeEnd)),
        };

        given.igniteClient(ignitePort)
                .testData(records);

        String sourceDDL = igniteTableBuilder()
                .withPartitionLowerBound(rangeStart)
                .withPartitionUpperBound(rangeEnd)
                .withPartitionColumn("day_date")
                .withTimezone(ZoneId.of("Europe/Warsaw"))
                .build();

        tableEnv.executeSql(sourceDDL);

        // when
        TableResult result =
                tableEnv.executeSql(
                        "SELECT name, SUM(weight) FROM ignite_source GROUP BY name");

        // then
        CloseableIterator<Row> collect = result.collect();
        List<Row> results = new ArrayList<>();
        collect.forEachRemaining(results::add);
        assertThat(results).hasSize(2)
                .extracting(row -> tuple(row.getField(0), row.getField(1)))
                .containsExactlyInAnyOrder(
                        tuple("bravo", bigDecimal(11)),
                        tuple("charlie", bigDecimal(12))
                );

    }

    private Timestamp timestamp(LocalDate day) {
        return new Timestamp(day.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli());
    }

    private BigDecimal bigDecimal(double value) {
        return BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP);
    }

    private IgniteSourceTableDDLBuilder igniteTableBuilder() {
        return new IgniteSourceTableDDLBuilder()
                .withTableName(tableName)
                .withColumnsDefinition(List.of("id INT NOT NULL",
                        "name STRING",
                        "weight DECIMAL(10,2)"))
                .withIgniteUrl("jdbc:ignite:thin://127.0.0.1:" + ignitePort)
                .withIgniteTableName(IgniteJdbcClient.tableName)
                .withUsername("ignite")
                .withPassword("ignite");
    }
}
