USE SCHEMA MTD;
CREATE or replace PROCEDURE proc_etl_link(INIT_FG VARCHAR)
 returns string
 language javascript
 as
   $$
var main = snowflake.createStatement({sqlText:`SELECT

'INSERT INTO EDW.'||link_table_name
||' ('||
substr(link_table_name,1,length(link_table_name)-position('_' IN reverse(link_table_name)))||'_HK , '
||hub_tables
||IFF(l.NON_HIST_LINK,nl_tgt_col,'')
||', KAFKA_DTS, LOAD_DTS, RECORD_SRC'
||') SELECT DISTINCT '
|| hk_l.ddl
||' as hash_key_calc, '
||hub_hk
||IFF(l.NON_HIST_LINK,nl_src_col||',to_timestamp_ltz(NVL(TRIM(RECORD_METADATA:LogAppendTime,''\\\'"\\\'''),TRIM(0)))',',min(to_timestamp_ltz(NVL(TRIM(RECORD_METADATA:LogAppendTime,''\\\'"\\\'''),current_timestamp)))')
||', to_timestamp_ltz(current_timestamp) AS LOAD_DTS,'
|| '''''' || upper(src_t.src_table_name) || ''''''
||' AS RECORD_SRC FROM (SELECT * FROM '
||IFF(`+INIT_FG+`,SRC_TABLE_DATABASE||'.'||src_table_schema||'.'||src_t.src_table_name,upper('STREAM_'||src_t.src_table_name|| '_TO_' || link_table_name))
||IFF(hk_l.array_path is not null and hk_l.array_path != '',',lateral flatten(input => '||IFF(SRC_VALUE_COLUMN is not null,TRIM(SRC_VALUE_COLUMN)||':','')||hk_l.array_path||')','')|| ' WHERE ' ||IFF(`+INIT_FG+`,' 1=1 ','METADATA$ACTION = ''\\\'INSERT\\\''' ') || IFF(hk_l.FILTER_CRITERIA <> 'PII',hk_l.FILTER_CRITERIA,'') ||' )src '
||IFF(l.NON_HIST_LINK,'',' WHERE hash_key_calc NOT IN ( SELECT ' ||substr(link_table_name,1,length(link_table_name)-position('_' IN reverse(link_table_name)))||'_HK FROM EDW.'|| link_table_name ||' ) GROUP BY hash_key_calc,' || hub_hk)

 AS out_etl_sql
,upper(link_table_name), upper(src_t.src_table_name), l.version_id, `+INIT_FG+`

FROM MTD.mtd_link l
/*Preperation of Link Hub Reference to get only one Row per Link with all Names and Hash Key Path prepared in an List*/
JOIN (SELECT lhr.link_table_id,lhr.src_table_id,lhr.version_id,
  listagg(substr(hub_table_name,1,length(hub_table_name)-position('_' IN reverse(hub_table_name)))||'_HK ',', ') as hub_tables,
  listagg(hk.ddl, ', ') as hub_hk
FROM MTD.mtd_link_hub_ref lhr
JOIN MTD.mtd_hub h
  ON lhr.hub_table_id = h.hub_table_id
  AND lhr.version_id = h.version_id
JOIN MTD.mtd_hash_key hk
  ON h.hub_table_name = hk.table_name
  AND lhr.src_table_id = hk.src_table_id
  AND lhr.version_id = hk.version_id  
GROUP BY lhr.link_table_id,lhr.src_table_id,lhr.version_id
) AS lhr
ON l.link_table_id = lhr.link_table_id
AND l.version_id = lhr.version_id

JOIN MTD.mtd_hash_key hk_l
ON l.link_table_name = hk_l.table_name
AND lhr.src_table_id = hk_l.src_table_id
AND l.version_id = hk_l.version_id

JOIN MTD.mtd_src_table src_t
  ON lhr.src_table_id = src_t.src_table_id
  AND l.version_id = src_t.version_id

/*Columns for NON_HIST_LINK - only needed if Flag is True*/
LEFT JOIN (
SELECT link_table_id, l_m.version_id, src_c.array_path, listagg(', '||link_column,'') as nl_tgt_col, listagg(CAST(IFF(upper(src_c.SRC_COLUMN_DATA_TYPE) = 'VARIANT',',',',TRIM(')||'src.'||IFF(src_c.array_path is not null,'value'|| IFF(src_column is not null,':'||TRIM(src_column),''),IFF(SRC_VALUE_COLUMN is not null,TRIM(SRC_VALUE_COLUMN)||':','')||TRIM(NVL(src_column,'')))||IFF(upper(src_c.SRC_COLUMN_DATA_TYPE) = 'VARIANT','',',''\\\'"\\\''''||')')  AS VARCHAR),'') as nl_src_col
FROM MTD.mtd_link_map l_m
JOIN MTD.mtd_src_column src_c
  ON l_m.src_table_id = src_c.src_table_id
  AND l_m.src_column_id = src_c.src_column_id
  AND l_m.version_id = src_c.version_id
JOIN MTD.mtd_src_table src_t
  ON l_m.src_table_id = src_t.src_table_id
  AND l_m.version_id = src_t.version_id
GROUP BY link_table_id, l_m.version_id, src_c.array_path
) AS l_m
ON l.link_table_id = l_m.link_table_id
AND l.version_id = l_m.version_id
AND l.NON_HIST_LINK = 'TRUE'

/*Check against Target Table if Same Record already exists*/
LEFT JOIN MTD.etl_sql es
  ON upper(link_table_name) = es.tgt_table_name
  AND upper(src_t.src_table_name) = es.src_table_name
  AND l.version_id = es.version_id 
  AND `+INIT_FG+` = es.init_fg

WHERE l.version_id = (select max(version_id) FROM MTD.MTD_MODEL_CREATION_LOG where approval = 1)
AND (es.etl_sql IS NULL OR es.etl_sql <> out_etl_sql);`});
  
  var main_val = main.execute();
  var counter =0;  
  while(main_val.next()) {
    var etl_sql = main_val.getColumnValue(1); 
    var tgt_table_name = main_val.getColumnValue(2);
    var src_table_name = main_val.getColumnValue(3);
    var version_id = main_val.getColumnValue(4);
    var init_fg = main_val.getColumnValue(5);
    var delete_etl_table = snowflake.createStatement({sqlText:`DELETE FROM etl_sql WHERE version_id = '`+version_id+`'AND src_table_name ='`+src_table_name+`'AND tgt_table_name = '`+tgt_table_name+`'AND init_fg = '`+init_fg+`';`});
    var delete_etl_table_exec = delete_etl_table.execute();
    var insert_etl_table = snowflake.createStatement({sqlText:`INSERT INTO etl_sql VALUES ('LINK','`+version_id+`','`+src_table_name+`','`+tgt_table_name+`','`+init_fg+`','`+etl_sql+`');`});
    var insert_etl_table_exec = insert_etl_table.execute();
    counter ++;
  }
 return counter;

 $$;