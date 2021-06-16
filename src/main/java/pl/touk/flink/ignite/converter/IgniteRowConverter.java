package pl.touk.flink.ignite.converter;

import org.apache.flink.connector.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

public class IgniteRowConverter extends AbstractJdbcRowConverter {

    public IgniteRowConverter(RowType rowType) {
        super(rowType);
    }

    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "Ignite";
    }

}
