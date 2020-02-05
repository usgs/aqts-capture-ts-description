package gov.usgs.wma.waterdata;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class TimeSeriesDescriptionDao {

	@Autowired
	protected JdbcTemplate jdbcTemplate;

	public int getTsdRecordCount() {
		return jdbcTemplate.queryForObject("select count(*) from time_series_description", Integer.class);
	}
	public int getJsonDataRecordCount() {
		return jdbcTemplate.queryForObject("select count(*) from json_data", Integer.class);
	}
	public String getTimeSeriesDescriptions() {
		return jdbcTemplate.queryForObject("select a.time_series_descriptions,\n" +
				"regexp_split_to_array(jsonb_extract_path_text(a.time_series_descriptions, 'Description'), ',') description,\n" +
				"a.script_pid,\n" +
				"a.start_time,\n" +
				"a.script_name,\n" +
				"a.json_data_id\n" +
				"from\n" +
				"(\n" +
				"\tselect jsonb_array_elements(jsonb_extract_path(json_content, 'TimeSeriesDescriptions')) time_series_descriptions,\n" +
				"\tscript_pid,\n" +
				"\tstart_time,\n" +
				"\tscript_name,\n" +
				"\tjson_data_id\n" +
				"\tfrom capture.json_data\n" +
				")a\n" +
				"where jsonb_extract_path_text(a.time_series_descriptions, 'ComputationPeriodIdentifier') = 'Daily'", String.class);
	}

	public int getTimeSeriesDescriptionsCount() {
		return jdbcTemplate.queryForObject("select count(*) from (\n" +
				"\n" +
				"select a.time_series_descriptions,\n" +
				"regexp_split_to_array(jsonb_extract_path_text(a.time_series_descriptions, 'Description'), ',') description,\n" +
				"a.script_pid,\n" +
				"a.start_time,\n" +
				"a.script_name,\n" +
				"a.json_data_id\n" +
				"from\n" +
				"(\n" +
				"\tselect jsonb_array_elements(jsonb_extract_path(json_content, 'TimeSeriesDescriptions')) time_series_descriptions,\n" +
				"\tscript_pid,\n" +
				"\tstart_time,\n" +
				"\tscript_name,\n" +
				"\tjson_data_id\n" +
				"\tfrom capture.json_data\n" +
				")a\n" +
				"where jsonb_extract_path_text(a.time_series_descriptions, 'ComputationPeriodIdentifier') = 'Daily'\n" +
				"\n" +
				") c", Integer.class);
	}
}
