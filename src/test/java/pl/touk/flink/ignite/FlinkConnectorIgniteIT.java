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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import pl.touk.flink.ignite.precondition.IgniteJdbcClient;
import pl.touk.flink.ignite.precondition.Preconditions;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

public class FlinkConnectorIgniteIT {

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
    static void tearDown() {
        ignite.close();
    }

    @Test
    void igniteConnectorTest() throws Exception {
        // given
        IgniteJdbcClient.TestRecord[] records = new IgniteJdbcClient.TestRecord[] {
                new IgniteJdbcClient.TestRecord(1, "alpha", BigDecimal.valueOf(10)),
                new IgniteJdbcClient.TestRecord(2, "alpha", BigDecimal.valueOf(11)),
                new IgniteJdbcClient.TestRecord(3, "bravo", BigDecimal.valueOf(5)),
        };

        given.igniteClient(ignitePort)
                .testData(records);

        String sourceDDL =
            String.format(
                    "CREATE TABLE ignite_source ("
                            + " id INT NOT NULL,"
                            + " name STRING,"
                            + " weight DECIMAL(10,2)"
                            + ") WITH ("
                            + " 'connector' = 'ignite',"
                            + " 'url' = 'jdbc:ignite:thin://127.0.0.1:%s',"
                            + " 'username' = 'ignite',"
                            + " 'password' = 'ignite',"
                            + " 'table-name' = '%s'"
                            + ")",
                    ignitePort,
                    "TEST");

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

    private BigDecimal bigDecimal(double value) {
        return BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP);
    }

}
