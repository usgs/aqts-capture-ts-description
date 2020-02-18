insert into
	time_series_description
(
	unit,
	label,
	time_series_unique_id,
	parameter,
	parm_cd,
	stat_cd,
	utc_offset,
	last_modified,
	corrected_start_time,
	corrected_end_time,
	location_identifier,
	computation_period_identifier,
	response_time,
	response_version
)
select
	jsonb_extract_path_text(time_series_descriptions, 'Unit') unit,
	jsonb_extract_path_text(time_series_descriptions, 'Label') as label,
	jsonb_extract_path_text(time_series_descriptions, 'UniqueId') time_series_unique_id,
	jsonb_extract_path_text(time_series_descriptions, 'Parameter') as parameter,
	description[2] parm_cd,
	description[4] stat_cd,
	jsonb_extract_path_text(time_series_descriptions, 'UtcOffset')::integer utc_offset,
	adjust_timestamp(jsonb_extract_path_text(time_series_descriptions, 'LastModified')) last_modified,
	adjust_timestamp(jsonb_extract_path_text(time_series_descriptions, 'CorrectedStartTime')) corrected_start_time,
	adjust_timestamp(jsonb_extract_path_text(time_series_descriptions, 'CorrectedEndTime')) corrected_end_time,
	jsonb_extract_path_text(time_series_descriptions, 'LocationIdentifier') location_identifier,
	jsonb_extract_path_text(time_series_descriptions, 'ComputationPeriodIdentifier') computation_period_identifier,
	response_time,
	response_version
from
(
	select a.time_series_descriptions,
	regexp_split_to_array(jsonb_extract_path_text(a.time_series_descriptions, 'Description'), ',') description,
	a.script_pid,
	a.start_time,
	a.script_name,
	a.response_time,
	a.response_version
	from
	(
		select jsonb_array_elements(jsonb_extract_path(json_content, 'TimeSeriesDescriptions')) time_series_descriptions,
		jsonb_extract_path_text(json_content, 'ResponseTime')::timestamp response_time,
		jsonb_extract_path_text(json_content, 'ResponseVersion')::integer response_version,
		script_pid,
		start_time,
		script_name
		from json_data
		where json_data_id = ?
	) as a
)
as b
on conflict (time_series_unique_id) do update
	set
		unit = excluded.unit,
		label = excluded.label,
		parameter = excluded.parameter,
		parm_cd = excluded.parm_cd,
		stat_cd = excluded.stat_cd,
		utc_offset = excluded.utc_offset,
		last_modified = excluded.last_modified,
		corrected_start_time = excluded.corrected_start_time,
		corrected_end_time = excluded.corrected_end_time,
		location_identifier = excluded.location_identifier,
		computation_period_identifier = excluded.computation_period_identifier,
		response_time = excluded.response_time,
		response_version = excluded.response_version
where time_series_description.last_modified < excluded.last_modified
returning time_series_unique_id
