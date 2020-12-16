CREATE or replace PROCEDURE mtd.proc_etl_sat(INIT_FG VARCHAR)
 returns string
 language javascript
 as
   $$
var main = snowflake.createStatement({sqlText:`SELECT
'INSERT INTO EDW.'||sat_table_name
||' ('||
sub_table.HK||' , '
||listagg(sat_column,', ')
||IFF(s.HASH_DIFF,', HASH_DIFF','')
||IFF(s.kafka_dts,', KAFKA_DTS', '')
||', LOAD_DTS, RECORD_SRC'
||') 
SELECT DISTINCT '
|| hk_s.ddl
||' as hash_key_calc ,'
||listagg(CAST(IFF(upper(src_c.SRC_COLUMN_DATA_TYPE) = 'VARIANT','','TRIM(')||'src.'||IFF(SRC_VALUE_COLUMN is not null,TRIM(SRC_VALUE_COLUMN)||':','')||TRIM(NVL(src_column,''))||IFF(upper(src_c.SRC_COLUMN_DATA_TYPE) = 'VARIANT','',',''\\\'"\\\''''||')')  AS VARCHAR),', ') || ', '
||IFF(s.HASH_DIFF,'upper(md5(upper('||listagg(CAST('NVL(TRIM(src.'||IFF(SRC_VALUE_COLUMN is not null,TRIM(SRC_VALUE_COLUMN)||':','')||TRIM(NVL(src_column,''))||','||'''\\\'"\\\'''' ||'),'||'''\\\'\\\'''' ||')'  AS VARCHAR),'||' || '''\\\';\\\'''' || '||')||'))) as hash_diff_calc ','')  
||IFF(kafka_dts, ', MIN(to_timestamp_ltz(NVL(TRIM(RECORD_METADATA:LogAppendTime,''\\\'"\\\'''),TRIM(0))))','')
||', to_timestamp_ltz(current_timestamp) ,'
|| '''''' || upper(src_t.src_table_name) || ''''''
||' 
  FROM (SELECT * FROM ' 
||IFF(`+INIT_FG+`,SRC_TABLE_DATABASE||'.'||src_table_schema||'.'||src_t.src_table_name,upper('STREAM_'||src_t.src_table_name|| '_TO_' || sat_table_name))
||IFF(hk_s.array_path is not null and hk_s.array_path != '',',lateral flatten(input => '||IFF(SRC_VALUE_COLUMN is not null,TRIM(SRC_VALUE_COLUMN)||':','')||hk_s.array_path||')','') || ' WHERE 1=1 ' || IFF(src_c.FILTER_CRITERIA <> 'PII',src_c.FILTER_CRITERIA,'') ||' )src '
||IFF(s.HASH_DIFF,' 
  LEFT JOIN ( SELECT '|| sub_table.HK ||', hash_diff 
                FROM EDW.'|| sat_table_name ||' as tgt 
                JOIN (SELECT '|| sub_table.HK ||' as max_hk ,max('|| IFF(s.kafka_dts, 'KAFKA_DTS', 'LOAD_DTS') ||') as MAX_LOAD_DTS FROM EDW.'|| sat_table_name ||' GROUP BY 1) as tgt_max
                  ON tgt.'|| sub_table.HK ||' = tgt_max.max_hk AND '|| IFF(s.kafka_dts, 'tgt.KAFKA_DTS', 'tgt.LOAD_DTS') ||' = tgt_max.MAX_LOAD_DTS ) tgt_out
         ON '|| hk_s.ddl ||' = '|| sub_table.HK ||' 
 WHERE (hash_diff_calc <> hash_diff OR hash_diff is null) AND ' || hk_s.null_logic ,' WHERE '|| hk_s.null_logic)
||IFF(s.kafka_dts,' 
 GROUP BY '|| hk_s.ddl||','
||listagg(CAST(IFF(upper(src_c.SRC_COLUMN_DATA_TYPE) = 'VARIANT','','TRIM(')||'src.'||IFF(SRC_VALUE_COLUMN is not null,TRIM(SRC_VALUE_COLUMN)||':','')||TRIM(NVL(src_column,''))||IFF(upper(src_c.SRC_COLUMN_DATA_TYPE) = 'VARIANT','',',''\\\'"\\\''''||')')  AS VARCHAR),', ') || ', '
||IFF(s.HASH_DIFF,'hash_diff_calc ','')  
||', to_timestamp_ltz(current_timestamp) ,'
|| '''''' || upper(src_t.src_table_name) || '''''', '' )
 AS out_etl_sql
,upper(sat_table_name), upper(src_t.src_table_name), s.version_id, `+INIT_FG+`
/*SAT Base Table and JOIN against HUB/LINK to find out Parent Element - LINK-SAT or HUB-SAT*/
FROM MTD.mtd_sat s
JOIN MTD.mtd_sat_map s_m
  ON s.sat_table_id = s_m.sat_table_id
  AND s.version_id = s_m.version_id
JOIN(
  SELECT hub_table_id, -1 as link_table_id ,version_id, upper(substr(hub_table_name,1,length(hub_table_name)-position('_' IN reverse(hub_table_name)))||'_HK') AS HK
  FROM MTD.MTD_HUB h
  UNION
  SELECT -1 as hub_table_id, link_table_id,version_id, upper(substr(link_table_name,1,length(link_table_name)-position('_' IN reverse(link_table_name)))||'_HK') AS HK
  FROM MTD.MTD_LINK l
) sub_table
  ON (sub_table.hub_table_id = s.HUB_TABLE_ID OR sub_table.link_table_id = s.link_table_id)
  AND s.version_id = sub_table.version_id
JOIN MTD.mtd_hash_key hk_s
  ON s.sat_table_name = hk_s.table_name
  AND s.src_table_id = hk_s.src_table_id
  AND s.version_id = hk_s.version_id
JOIN MTD.mtd_src_table src_t
  ON s.src_table_id = src_t.src_table_id
  AND s.version_id = src_t.version_id
JOIN MTD.mtd_src_column src_c
  ON s.src_table_id = src_c.src_table_id
  AND s_m.src_column_id = src_c.src_column_id
  AND s_m.version_id = src_c.version_id
/*Check against Target Table if Same Record already exists*/
LEFT JOIN MTD.etl_sql es
  ON upper(sat_table_name) = es.tgt_table_name
  AND upper(src_t.src_table_name) = es.src_table_name
  AND s.version_id = es.version_id  
  AND `+INIT_FG+` = es.init_fg  
WHERE s.version_id = (select max(version_id) FROM MTD.MTD_MODEL_CREATION_LOG where approval = 1)
GROUP BY sat_table_name,src_t.src_table_database,src_t.src_table_schema,src_t.src_table_name,s.src_table_id, s.version_id, s.kafka_dts, SUB_TABLE.HK,s.HASH_DIFF,HK_S.DDL,es.etl_sql,hk_s.array_path,SRC_T.SRC_VALUE_COLUMN,hk_s.null_logic,src_c.FILTER_CRITERIA
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
    var insert_etl_table = snowflake.createStatement({sqlText:`INSERT INTO etl_sql VALUES ('SAT','`+version_id+`','`+src_table_name+`','`+tgt_table_name+`','`+init_fg+`','`+etl_sql+`');`});
    var insert_etl_table_exec = insert_etl_table.execute();
    counter ++;
  }
 return counter;

 $$;