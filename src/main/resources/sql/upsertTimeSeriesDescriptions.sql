insert into
	capture.time_series_description
(
	unit,
	label,
	time_series_unique_id,
	parameter,
	parm_cd,
	stat_cd,
	last_modified,
	corrected_start_time,
	corrected_end_time,
	location_identifier,
	computation_period_identifier
)
select
	jsonb_extract_path_text(time_series_descriptions, 'Unit') unit,
	jsonb_extract_path_text(time_series_descriptions, 'Label') as label,
	jsonb_extract_path_text(time_series_descriptions, 'UniqueId') time_series_unique_id,
	jsonb_extract_path_text(time_series_descriptions, 'Parameter') as parameter,
	description[2] parm_cd,
	description[4] stat_cd,
	jsonb_extract_path_text(time_series_descriptions, 'LastModified')::timestamp last_modified,
	jsonb_extract_path_text(time_series_descriptions, 'CorrectedStartTime')::timestamp corrected_start_time,
	jsonb_extract_path_text(time_series_descriptions, 'CorrectedEndTime')::timestamp corrected_end_time,
	jsonb_extract_path_text(time_series_descriptions, 'LocationIdentifier') location_identifier,
	jsonb_extract_path_text(time_series_descriptions, 'ComputationPeriodIdentifier') computation_period_identifier
from
(
	select a.time_series_descriptions,
	regexp_split_to_array(jsonb_extract_path_text(a.time_series_descriptions, 'Description'), ',') description,
	a.script_pid,
	a.start_time,
	a.script_name,
	a.json_data_id
	from
	(
		select jsonb_array_elements(jsonb_extract_path(json_content, 'TimeSeriesDescriptions')) time_series_descriptions,
		script_pid,
		start_time,
		script_name,
		json_data_id,
		url
		from capture.json_data
	) as a
	where jsonb_extract_path_text(a.time_series_descriptions, 'ComputationPeriodIdentifier') = 'Daily'
	and a.url like '%GetTimeSeriesDescriptionListByUniqueId%'
	and a.json_data_id = ?
)
as b
on conflict (time_series_unique_id) do update
	set
		unit = excluded.unit,
		label = excluded.label,
		parameter = excluded.parameter,
		parm_cd = excluded.parm_cd,
		stat_cd = excluded.stat_cd,
		last_modified = excluded.last_modified,
		corrected_start_time = excluded.corrected_start_time,
		corrected_end_time = excluded.corrected_end_time,
		location_identifier = excluded.location_identifier,
		computation_period_identifier = excluded.computation_period_identifier
where capture.time_series_description.last_modified < excluded.last_modified
returning time_series_unique_id