package gov.usgs.wma.waterdata;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class TimeSeriesDescriptionDao {

	@Autowired
	protected JdbcTemplate jdbcTemplate;

	public int getRecordCount() {
		return jdbcTemplate.queryForObject("select count(*) from time_series_description", Integer.class);
	}

}
