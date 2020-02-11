package gov.usgs.wma.waterdata;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;

import java.io.IOException;
import java.util.List;

@Component
public class TimeSeriesDescriptionDao {

	@Autowired
	protected JdbcTemplate jdbcTemplate;

	@Value("classpath:sql/upsertTimeSeriesDescriptions.sql")
	protected Resource upsertTimeSeriesDescriptions;

	public List<String> upsertTimeSeriesDescriptionsForSingleJsonDataId(Long jsonDataId) throws IOException {

		String sql = new String(FileCopyUtils.copyToByteArray(upsertTimeSeriesDescriptions.getInputStream()));
		Object[] theJsonDataId = {jsonDataId};
		return jdbcTemplate.queryForList(
				sql,
				theJsonDataId,
				String.class);
	}
}
