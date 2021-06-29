package pl.touk.flink.ignite.precondition;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

public class IgniteJdbcClient {

    public static final String tableName = "TEST";
    private final int port;

    public IgniteJdbcClient(int port) {
        this.port = port;
    }

    public void schemaCreated() throws Exception {
        execute("CREATE TABLE IF NOT EXISTS " + tableName + " (ID INT PRIMARY KEY, NAME VARCHAR, WEIGHT DECIMAL, DAY_DATE TIMESTAMP);");
    }

    public static class TestRecord {
        private final int id;
        private final String name;
        private final BigDecimal weight;
        private final Timestamp day;

        public TestRecord(int id, String name, BigDecimal weight, Timestamp day) {
            this.id = id;
            this.name = name;
            this.weight = weight;
            this.day = day;
        }
    }

    public void testData(TestRecord... records) throws Exception {
        withConnection(conn -> {
            try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM " + tableName)) {
                stmt.execute();
            }
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO " + tableName + " VALUES(?, ?, ?, ?);")) {
                for (TestRecord record : records) {
                    stmt.setInt(1, record.id);
                    stmt.setString(2, record.name);
                    stmt.setBigDecimal(3, record.weight);
                    stmt.setTimestamp(4, record.day);
                    stmt.execute();
                }
            }
        });
    }

    private interface Callback<T> {
        void call(T arg) throws Exception;
    }

    private void withConnection(Callback<Connection> body) throws Exception {
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        try(Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:" + port)) {
            body.call(conn);
        }
    }

    private void execute(String sql) throws Exception {
        withConnection(conn -> {
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.execute();
            }
        });
    }
}
