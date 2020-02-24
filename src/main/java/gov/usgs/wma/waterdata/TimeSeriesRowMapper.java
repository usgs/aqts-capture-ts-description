package gov.usgs.wma.waterdata;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

public class TimeSeriesRowMapper implements RowMapper<TimeSeries> {

	@Override
	public TimeSeries mapRow(ResultSet rs, int rowNum) throws SQLException {
		TimeSeries timeSeries = new TimeSeries(rs.getString("time_series_unique_id"));
		return timeSeries;
	}

}
