package gov.usgs.wma.waterdata;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class TimeSeriesDescriptionDao {

	@Autowired
	protected JdbcTemplate jdbcTemplate;

	public List<String> upsertTimeSeriesDescriptionsForSingleJsonDataId(Long jsonDataId) {

		String sql = "insert into \n" +
				"\tcapture.time_series_description \n" +
				"(\n" +
				"\tunit, \n" +
				"\tlabel, \n" +
				"\ttime_series_unique_id, \n" +
				"\tparameter, \n" +
				"\tparm_cd, \n" +
				"\tstat_cd, \n" +
				"\tlast_modified, \n" +
				"\tcorrected_start_time, \n" +
				"\tcorrected_end_time, \n" +
				"\tlocation_identifier, \n" +
				"\tcomputation_period_identifier\n" +
				")\n" +
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
				"\t) as a\n" +
				"\twhere jsonb_extract_path_text(a.time_series_descriptions, 'ComputationPeriodIdentifier') = 'Daily'\n" +
				"\tand a.url like '%GetTimeSeriesDescriptionListByUniqueId%'\n" +
				"\tand a.json_data_id = ? \n" +
				") \n" +
				"as b \n" +
				"on conflict (time_series_unique_id) do update \n" +
				"\tset \n" +
				"\t\tunit = excluded.unit,\n" +
				"\t\tlabel = excluded.label,  \n" +
				"\t\tparameter = excluded.parameter, \n" +
				"\t\tparm_cd = excluded.parm_cd, \n" +
				"\t\tstat_cd = excluded.stat_cd, \n" +
				"\t\tlast_modified = excluded.last_modified, \n" +
				"\t\tcorrected_start_time = excluded.corrected_start_time, \n" +
				"\t\tcorrected_end_time = excluded.corrected_end_time, \n" +
				"\t\tlocation_identifier = excluded.location_identifier, \n" +
				"\t\tcomputation_period_identifier = excluded.computation_period_identifier\n" +
				"where capture.time_series_description.last_modified < excluded.last_modified\n" +
				"returning time_series_unique_id";

		Object[] theJsonDataId = {jsonDataId};
		return jdbcTemplate.queryForList(sql, theJsonDataId, String.class);
	}
}
