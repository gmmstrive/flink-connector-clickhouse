package site.gaoxiaoming.converter;

import org.apache.flink.connector.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.types.logical.RowType;

/**
 * @author lucas
 */
public class ClickHouseRowConverter extends AbstractJdbcRowConverter {

    public ClickHouseRowConverter(RowType rowType) {
        super(rowType);
    }

    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "ClickHouse";
    }

}