package pl.touk.flink.ignite.precondition;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class IgniteJdbcClient {

    private final int port;

    public IgniteJdbcClient(int port) {
        this.port = port;
    }

    public void schemaCreated() throws Exception {
        execute("CREATE TABLE TEST (ID INT PRIMARY KEY, NAME VARCHAR, WEIGHT DECIMAL);");
    }

    public static class TestRecord {
        private final int id;
        private final String name;
        private final BigDecimal weight;

        public TestRecord(int id, String name, BigDecimal weight) {
            this.id = id;
            this.name = name;
            this.weight = weight;
        }
    }

    public void testData(TestRecord... records) throws Exception {
        String sql = "INSERT INTO TEST VALUES(?, ?, ?);";
        withConnection(conn -> {
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (TestRecord record : records) {
                    stmt.setInt(1, record.id);
                    stmt.setString(2, record.name);
                    stmt.setBigDecimal(3, record.weight);
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
