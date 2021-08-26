package pl.touk.flink.ignite.dialect;

import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;
import pl.touk.flink.ignite.converter.IgniteRowConverter;

import java.util.Optional;

public class IgniteDialect implements JdbcDialect {

    private static final long serialVersionUID = 1L;

    @Override
    public String dialectName() {
        return "Ignite";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:ignite:thin");
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new IgniteRowConverter(rowType);
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("org.apache.ignite.IgniteJdbcThinDriver");
    }

}
