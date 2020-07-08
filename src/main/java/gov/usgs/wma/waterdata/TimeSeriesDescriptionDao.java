package gov.usgs.wma.waterdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Component
public class TimeSeriesDescriptionDao {

	private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesDescriptionDao.class);

	@Autowired
	protected JdbcTemplate jdbcTemplate;

	@Value("classpath:sql/upsertTimeSeriesDescriptions.sql")
	protected Resource sqlQuery;

	public List<TimeSeries> upsertTimeSeriesDescription(Long jsonDataId, Integer partitionNumber) {
		List<TimeSeries> timeSeriesList = Arrays.asList();
		try {
			String sql = new String(FileCopyUtils.copyToByteArray(sqlQuery.getInputStream()));
			timeSeriesList = jdbcTemplate.query(
					sql,
					new TimeSeriesRowMapper(),
					jsonDataId,
					partitionNumber
					);
		} catch (EmptyResultDataAccessException e) {
			LOG.info("Couldn't find {} - {} ", jsonDataId, e.getLocalizedMessage());
		} catch (IOException e) {
			LOG.error("Unable to get SQL statement", e);
			throw new RuntimeException(e);
		}
		return timeSeriesList;
	}
}
