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
    response_version,
    extended_attributes,
    computation_identifier
)
select
    b.unit,
    b.label,
    b.time_series_unique_id,
    b.parameter,
    aq_to_nwis_parm.parm_cd,
    aq_comp_id_to_stat_cd.stat_cd,
    b.utc_offset,
    b.last_modified,
    b.corrected_start_time,
    b.corrected_end_time,
    b.location_identifier,
    b.computation_period_identifier,
    b.response_time,
    b.response_version,
    b.extended_attributes,
    b.computation_identifier
from
(
    select
        jsonb_extract_path_text(time_series_descriptions, 'Unit') unit,
        jsonb_extract_path_text(time_series_descriptions, 'Label') as label,
        jsonb_extract_path_text(time_series_descriptions, 'UniqueId') time_series_unique_id,
        jsonb_extract_path_text(time_series_descriptions, 'Parameter') as parameter,
        jsonb_extract_path_text(time_series_descriptions, 'UtcOffset')::numeric utc_offset,
        adjust_timestamp(jsonb_extract_path_text(time_series_descriptions, 'LastModified')) last_modified,
        adjust_timestamp(jsonb_extract_path_text(time_series_descriptions, 'CorrectedStartTime')) corrected_start_time,
        adjust_timestamp(jsonb_extract_path_text(time_series_descriptions, 'CorrectedEndTime')) corrected_end_time,
        jsonb_extract_path_text(time_series_descriptions, 'LocationIdentifier') location_identifier,
        jsonb_extract_path_text(time_series_descriptions, 'ComputationPeriodIdentifier') computation_period_identifier,
        a.response_time,
        a.response_version,
        jsonb_extract_path(a.time_series_descriptions, 'ExtendedAttributes') extended_attributes,
        jsonb_extract_path_text(time_series_descriptions, 'ComputationIdentifier') computation_identifier
    from
    (
        select jsonb_array_elements(jsonb_extract_path(json_content, 'TimeSeriesDescriptions')) time_series_descriptions,
        jsonb_extract_path_text(json_content, 'ResponseTime')::timestamp response_time,
        jsonb_extract_path_text(json_content, 'ResponseVersion')::integer response_version
        from json_data
        where json_data_id = ?
    ) as a
)
as b
   left join aq_comp_id_to_stat_cd
     on b.computation_identifier = aq_comp_id_to_stat_cd.computation_identifier
   left join aq_to_nwis_parm
     on b.parameter || '|' || b.unit = aq_to_nwis_parm.parameter
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
        response_version = excluded.response_version,
        extended_attributes = excluded.extended_attributes,
        computation_identifier = excluded.computation_identifier
where time_series_description.last_modified < excluded.last_modified
returning time_series_unique_id
