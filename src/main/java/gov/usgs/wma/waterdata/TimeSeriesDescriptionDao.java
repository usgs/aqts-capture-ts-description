package gov.usgs.wma.waterdata;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class TimeSeriesDescriptionDao {

	@Autowired
	protected JdbcTemplate jdbcTemplate;

	public int getTimeSeriesDescriptionsForSingleJsonDataIdCount() {
		return jdbcTemplate.queryForObject("select count(*) from (\n" +
				"select \n" +
				"\tjsonb_extract_path_text(time_series_descriptions, 'Label') as label,\n" +
				"\tjsonb_extract_path_text(time_series_descriptions, 'Unit') unit,\n" +
				"\tjsonb_extract_path_text(time_series_descriptions, 'UniqueId') time_series_unique_id,\n" +
				"\tjsonb_extract_path_text(time_series_descriptions, 'Parameter') as parameter,\n" +
				"\tdescription[2] parm_cd,\n" +
				"\tdescription[4] stat_cd,\n" +
				"\tjsonb_extract_path_text(time_series_descriptions, 'LastModified')::timestamp last_modified,\n" +
				"\tjsonb_extract_path_text(time_series_descriptions, 'CorrectedStartTime')::timestamp corrected_start_time,\n" +
				"\tjsonb_extract_path_text(time_series_descriptions, 'CorrectedEndTime')::timestamp corrected_end_time,\n" +
				"\tjsonb_extract_path_text(time_series_descriptions, 'LocationIdentifier') location_identifier,\n" +
				"\tjsonb_extract_path_text(time_series_descriptions, 'ComputationPeriodIdentifier') computation_period_identifier\n" +
				"from\n" +
				"(\n" +
				"\tselect a.time_series_descriptions,\n" +
				"\tregexp_split_to_array(jsonb_extract_path_text(a.time_series_descriptions, 'Description'), ',') description,\n" +
				"\ta.script_pid,\n" +
				"\ta.start_time,\n" +
				"\ta.script_name,\n" +
				"\ta.json_data_id\n" +
				"\tfrom\n" +
				"\t(\n" +
				"\t\tselect jsonb_array_elements(jsonb_extract_path(json_content, 'TimeSeriesDescriptions')) time_series_descriptions,\n" +
				"\t\tscript_pid,\n" +
				"\t\tstart_time,\n" +
				"\t\tscript_name,\n" +
				"\t\tjson_data_id,\n" +
				"\t\turl\n" +
				"\t\tfrom capture.json_data\n" +
				"\t)a\n" +
				"\twhere jsonb_extract_path_text(a.time_series_descriptions, 'ComputationPeriodIdentifier') = 'Daily'\n" +
				"\tand a.url like '%GetTimeSeriesDescriptionListByUniqueId%'\n" +
				"\tand a.json_data_id = 199\n" +
				") b \n" +
				") c", Integer.class);
	}

	public void insertTimeSeriesDescriptionsForSingleJsonDataId() {
		jdbcTemplate.execute("insert into capture.time_series_description (unit, label, time_series_unique_id, parameter, parm_cd, stat_cd, last_modified, corrected_start_time, corrected_end_time, location_identifier, computation_period_identifier)\n" +
				"(\n" +
				"select \n" +
				"\tjsonb_extract_path_text(time_series_descriptions, 'Unit') unit,\n" +
				"\tjsonb_extract_path_text(time_series_descriptions, 'Label') as label,\n" +
				"\tjsonb_extract_path_text(time_series_descriptions, 'UniqueId') time_series_unique_id,\n" +
				"\tjsonb_extract_path_text(time_series_descriptions, 'Parameter') as parameter,\n" +
				"\tdescription[2] parm_cd,\n" +
				"\tdescription[4] stat_cd,\n" +
				"\tjsonb_extract_path_text(time_series_descriptions, 'LastModified')::timestamp last_modified,\n" +
				"\tjsonb_extract_path_text(time_series_descriptions, 'CorrectedStartTime')::timestamp corrected_start_time,\n" +
				"\tjsonb_extract_path_text(time_series_descriptions, 'CorrectedEndTime')::timestamp corrected_end_time,\n" +
				"\tjsonb_extract_path_text(time_series_descriptions, 'LocationIdentifier') location_identifier,\n" +
				"\tjsonb_extract_path_text(time_series_descriptions, 'ComputationPeriodIdentifier') computation_period_identifier\n" +
				"from\n" +
				"(\n" +
				"\tselect a.time_series_descriptions,\n" +
				"\tregexp_split_to_array(jsonb_extract_path_text(a.time_series_descriptions, 'Description'), ',') description,\n" +
				"\ta.script_pid,\n" +
				"\ta.start_time,\n" +
				"\ta.script_name,\n" +
				"\ta.json_data_id\n" +
				"\tfrom\n" +
				"\t(\n" +
				"\t\tselect jsonb_array_elements(jsonb_extract_path(json_content, 'TimeSeriesDescriptions')) time_series_descriptions,\n" +
				"\t\tscript_pid,\n" +
				"\t\tstart_time,\n" +
				"\t\tscript_name,\n" +
				"\t\tjson_data_id,\n" +
				"\t\turl\n" +
				"\t\tfrom capture.json_data\n" +
				"\t)a\n" +
				"\twhere jsonb_extract_path_text(a.time_series_descriptions, 'ComputationPeriodIdentifier') = 'Daily'\n" +
				"\tand a.url like '%GetTimeSeriesDescriptionListByUniqueId%'\n" +
				"\tand a.json_data_id = 199\n" +
				") b\n" +
				")");
	}
}
