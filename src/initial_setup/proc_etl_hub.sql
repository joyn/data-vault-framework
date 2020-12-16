CREATE or replace PROCEDURE mtd.proc_etl_hub(INIT_FG VARCHAR)
 returns string
 language javascript
 as
   $$
var main = snowflake.createStatement({sqlText:`SELECT
'INSERT INTO EDW.'|| hub_table_name ||' ('||
substr(hub_table_name,1,length(hub_table_name)-position('_' IN reverse(hub_table_name)))||'_HK, ' ||listagg(hub_column,', ')
||IFF(kafka_dts,', KAFKA_DTS', '')
||', LOAD_DTS, RECORD_SRC'
||') 
SELECT '
|| hk.ddl
||' as hash_key_calc, '
|| listagg(CAST('MIN(TRIM(src.'||IFF(src_c.array_path is not null,'value'|| IFF(src_column is not null,':'||TRIM(src_column),''),IFF(SRC_VALUE_COLUMN is not null,TRIM(SRC_VALUE_COLUMN)||':','')||TRIM(NVL(src_column,'')))||','||'''\\\'"\\\'''' ||'))' AS VARCHAR),', ')
|| IFF(kafka_dts, ', min(to_timestamp_ltz(NVL(TRIM(RECORD_METADATA:LogAppendTime,''\\\'"\\\'''),TRIM(0))))','')
||', to_timestamp_ltz(current_timestamp) as LOAD_DTS,'
|| '''''' || upper(src_t.src_table_name) || ''''''
||' AS RECORD_SRC 
  FROM (SELECT * 
          FROM '
||IFF(`+INIT_FG+`,SRC_TABLE_DATABASE||'.'||src_table_schema||'.'||src_t.src_table_name,upper('STREAM_'||src_t.src_table_name|| '_TO_' || hub_table_name))
||IFF(src_c.array_path is not null,',lateral flatten(input => '||IFF(SRC_VALUE_COLUMN is not null,TRIM(SRC_VALUE_COLUMN)||':','')||src_c.array_path||')','') || ' 
         WHERE 1=1 ' || IFF(src_c.FILTER_CRITERIA <> 'PII',src_c.FILTER_CRITERIA,'') ||' ) src 
 WHERE hash_key_calc NOT IN (SELECT '||substr(hub_table_name,1,length(hub_table_name)-position('_' IN reverse(hub_table_name)))||'_HK'||' FROM EDW.'|| hub_table_name || ') 
 GROUP BY hash_key_calc, LOAD_DTS,  RECORD_SRC'
 AS out_etl_sql
,upper(hub_table_name), upper(src_t.src_table_name), h.version_id, `+INIT_FG+`
FROM MTD.mtd_hub h
JOIN  MTD.mtd_hub_map h_m
ON h.hub_table_id = h_m.hub_table_id
AND h.version_id = h_m.version_id
JOIN MTD.mtd_src_table src_t
ON h_m.src_table_id = src_t.src_table_id
AND h_m.version_id = src_t.version_id
JOIN MTD.mtd_src_column src_c
ON h_m.src_table_id = src_c.src_table_id
AND h_m.src_column_id = src_c.src_column_id
AND h_m.version_id = src_c.version_id
JOIN MTD.mtd_hash_key hk
ON h.hub_table_name = hk.table_name
AND h_m.src_table_id = hk.src_table_id
AND h_m.version_id = hk.version_id
/*Check against Target Table if Same Record already exists*/
LEFT JOIN MTD.etl_sql es
  ON upper(hub_table_name) = es.tgt_table_name
  AND upper(src_t.src_table_name) = es.src_table_name
  AND h.version_id = es.version_id   
  AND `+INIT_FG+` = es.init_fg
WHERE h.version_id = (select max(version_id) FROM MTD.MTD_MODEL_CREATION_LOG where approval = 1)
GROUP BY hub_table_name,src_t.src_table_database,src_t.src_table_schema,src_t.src_table_name,h_m.src_table_id, h.version_id, h.kafka_dts, hk.ddl,es.etl_sql,src_t.src_value_column,src_c.array_path,src_c.FILTER_CRITERIA
HAVING es.etl_sql IS NULL OR es.etl_sql <> out_etl_sql`});
  
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
    var insert_etl_table = snowflake.createStatement({sqlText:`INSERT INTO etl_sql VALUES ('HUB','`+version_id+`','`+src_table_name+`','`+tgt_table_name+`','`+init_fg+`','`+etl_sql+`');`});
    var insert_etl_table_exec = insert_etl_table.execute();
    counter ++;
  }
 return counter;

 $$;