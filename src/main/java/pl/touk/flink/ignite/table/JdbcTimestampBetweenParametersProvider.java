package pl.touk.flink.ignite.table;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

@Experimental
public class JdbcTimestampBetweenParametersProvider implements JdbcParameterValuesProvider {

	private final ZoneId timezone;
	private final LocalDate minVal;
	private final LocalDate maxVal;

	public JdbcTimestampBetweenParametersProvider(ZoneId timezone, LocalDate minVal, LocalDate maxVal) {
		this.timezone = timezone;
		this.minVal = minVal;
		this.maxVal = maxVal;
	}

	@Override
	public Serializable[][] getParameterValues() {
		int batchNum = Long.valueOf(minVal.until(maxVal, ChronoUnit.DAYS) + 1).intValue();
		Serializable[][] parameters = new Serializable[batchNum][2];
		for (int i = 0; i < batchNum; i++) {
			LocalDate current = minVal.plusDays(i);
			Instant startInstant = current.atStartOfDay().atZone(timezone).toInstant();
			Instant endInstant = current.atTime(LocalTime.MAX).atZone(timezone).toInstant();
			Timestamp start = new Timestamp(startInstant.toEpochMilli());
			Timestamp end = new Timestamp(endInstant.toEpochMilli());
			parameters[i] = new Timestamp[]{start, end};
		}
		return parameters;
	}
}
