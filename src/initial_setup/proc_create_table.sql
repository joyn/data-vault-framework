CREATE or replace PROCEDURE mtd.proc_create_tables(CATEGORY STRING)
 returns float
 language javascript
 as
   $$
   if (CATEGORY == 'hub') {
   var list_hub_stmt = snowflake.createStatement({sqlText:`SELECT hub_table_name, upper(
'CREATE OR REPLACE TABLE EDW.'||hub_table_name
||' ('||
substr(hub_table_name,1,length(hub_table_name)-position('_' IN reverse(hub_table_name)))||'_HK varchar, '
||listagg(hub_column||' '||src_column_data_type,', ')
||IFF(kafka_dts, ', KAFKA_DTS TIMESTAMP_LTZ', '')
||', LOAD_DTS TIMESTAMP_LTZ,  RECORD_SRC varchar'
||')'
) AS DDL_CMD
FROM MTD.mtd_hub h
JOIN ( SELECT DISTINCT h_m.hub_table_id, h_m.hub_column, src_c.src_column_data_type, h_m.version_id, hub_column_order_id
  FROM MTD.mtd_hub_map h_m
  JOIN MTD.mtd_src_column src_c
  ON h_m.src_table_id = src_c.src_table_id
  AND h_m.src_column_id = src_c.src_column_id
  AND h_m.version_id = src_c.version_id
  ORDER BY hub_column_order_id
) AS h_m
ON h.hub_table_id = h_m.hub_table_id
AND h.version_id = h_m.version_id
/*Logic to identify Changes in the Scructure of the Table - Record will only get generated if following Logic finds difference between current Table and already existing*/
JOIN (
WITH
info_schema AS ( SELECT upper(TABLE_NAME) AS TABLE_NAME, upper(COLUMN_NAME) AS COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS col
    JOIN MTD.mtd_hub h
    ON upper(hub_table_name) = upper(col.TABLE_NAME)
    AND h.version_id = (select max(version_id) FROM MTD.MTD_MODEL_CREATION_LOG where approval = 1)
    where col.COLUMN_NAME NOT IN ('RECORD_SRC','KAFKA_DTS','LOAD_DTS',upper(substr(hub_table_name,1,length(hub_table_name)-position('_' IN reverse(hub_table_name)))||'_HK'))
                ),
metadata_table AS ( SELECT upper(hub_table_name) AS TABLE_NAME, upper(hub_column) AS COLUMN_NAME
    FROM MTD.mtd_hub h
    JOIN  MTD.mtd_hub_map h_m
    ON h.hub_table_id = h_m.hub_table_id
    AND h.version_id = h_m.version_id
    where h.version_id = (select max(version_id) FROM MTD.MTD_MODEL_CREATION_LOG where approval = 1))
    
SELECT DISTINCT table_name  FROM 
(   
  /*Changed Columns*/
  (SELECT * FROM info_schema  
  MINUS
  SELECT * FROM metadata_table)
  
  UNION ALL
  
  /*New Tables/Columns*/
  (SELECT * FROM metadata_table
  MINUS
  SELECT * FROM info_schema)
)
) AS change_check
ON change_check.TABLE_NAME = upper(hub_table_name)
where h.version_id = (select max(version_id) FROM MTD.MTD_MODEL_CREATION_LOG where approval = 1)
GROUP BY hub_table_name,kafka_dts;`});

 var tables = list_hub_stmt.execute();
 var counter =0;
 while(tables.next()) {
     var ddl_query = tables.getColumnValue(2);
     var ddl_stmt = snowflake.createStatement({sqlText: ddl_query});
     ddl_stmt.execute();
     counter ++;
 }
 return counter;
 }
 else if (CATEGORY == 'link') {
 var list_link_stmt=snowflake.createStatement({sqlText:`SELECT link_table_name, upper(
   'CREATE OR REPLACE TABLE EDW.'||link_table_name
   ||' ('||
   substr(link_table_name,1,length(link_table_name)-position('_' IN reverse(link_table_name)))||'_HK varchar, '
   ||hub_table_hk||', '
   ||IFF(l.NON_HIST_LINK,listagg(link_column||' '||src_column_data_type||', ',''),'')
   ||IFF(l.kafka_dts, 'KAFKA_DTS TIMESTAMP_LTZ ,', '')
   ||' LOAD_DTS TIMESTAMP_LTZ, RECORD_SRC varchar'
   ||')'
   ) AS DDL_CMD
   FROM MTD.mtd_link l
   JOIN (   SELECT link_table_id,
         listagg(substr(hub_table_name,1,length(hub_table_name)-position('_' IN reverse(hub_table_name)))||'_HK'||' varchar',', ') within group (order by hub_order_id, hub_table_name) as hub_table_hk
         ,version_id 
         FROM (SELECT DISTINCT link_table_id,hub_table_name,hub_order_id,lhr.version_id 
   FROM MTD.mtd_link_hub_ref lhr
   JOIN MTD.mtd_hub h
   ON lhr.hub_table_id = h.hub_table_id
   AND lhr.version_id = h.version_id)
   GROUP BY link_table_id, version_id 
   ) AS lhr
   ON l.link_table_id = lhr.link_table_id
   AND l.version_id = lhr.version_id
   LEFT JOIN (
     SELECT link_table_id, link_column, src_column_data_type, l_m.version_id
     FROM MTD.mtd_link_map l_m
     JOIN MTD.mtd_src_column src_c
     ON l_m.src_table_id = src_c.src_table_id
     AND l_m.src_column_id = src_c.src_column_id
     AND l_m.version_id = src_c.version_id
   ) AS l_m
   ON l.link_table_id = l_m.link_table_id
   AND l.version_id = l_m.version_id
   AND l.NON_HIST_LINK = 'TRUE'
/*Logic to identify Changes in the Scructure of the Table - Record will only get generated if following Logic finds difference between current Table and already existing*/
JOIN (
WITH
info_schema AS ( SELECT upper(TABLE_NAME) AS TABLE_NAME, upper(COLUMN_NAME) AS COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS col
    JOIN MTD.mtd_link l
    ON upper(link_table_name) = upper(col.TABLE_NAME)
    AND l.version_id = (select max(version_id) FROM MTD.MTD_MODEL_CREATION_LOG where approval = 1)
    where col.COLUMN_NAME NOT IN ('RECORD_SRC','KAFKA_DTS','LOAD_DTS',upper(substr(link_table_name,1,length(link_table_name)-position('_' IN reverse(link_table_name)))||'_HK'))
                ),
metadata_table AS ( SELECT  upper(link_table_name) AS TABLE_NAME, upper(hub_column) AS COLUMN_NAME
    FROM MTD.mtd_link l
   JOIN ( SELECT link_table_id,upper(substr(hub_table_name,1,length(hub_table_name)-position('_' IN reverse(hub_table_name)))||'_HK') as hub_column,lhr.version_id 
   FROM MTD.mtd_link_hub_ref lhr
   JOIN MTD.mtd_hub h
   ON lhr.hub_table_id = h.hub_table_id
   AND lhr.version_id = h.version_id
   ) AS lhr
   ON l.link_table_id = lhr.link_table_id
   AND l.version_id = lhr.version_id
   where l.version_id = (select max(version_id) FROM MTD.MTD_MODEL_CREATION_LOG where approval = 1)
   UNION
SELECT  upper(link_table_name) AS TABLE_NAME, upper(link_column) AS COLUMN_NAME
    FROM MTD.mtd_link l
    JOIN  MTD.mtd_link_map l_m
    ON l.link_table_id = l_m.link_table_id 
    AND l.version_id = l_m.version_id
    where l.version_id = (select max(version_id) FROM MTD.MTD_MODEL_CREATION_LOG where approval = 1))
    
SELECT DISTINCT table_name  FROM 
(   
  /*Changed Columns*/
  (SELECT * FROM info_schema  
  MINUS
  SELECT * FROM metadata_table)
  
  UNION ALL
  
  /*New Tables/Columns*/
  (SELECT * FROM metadata_table
  MINUS
  SELECT * FROM info_schema)
)
) AS change_check
ON change_check.TABLE_NAME = upper(link_table_name)
where l.version_id = (select max(version_id) FROM MTD.MTD_MODEL_CREATION_LOG where approval = 1)
  GROUP BY link_table_name,l.NON_HIST_LINK,hub_table_hk,kafka_dts;`});
  var tables = list_link_stmt.execute();
  var counter =0;
  while(tables.next()) {
      var ddl_query = tables.getColumnValue(2);
      var ddl_stmt = snowflake.createStatement({sqlText: ddl_query});
      ddl_stmt.execute();
      counter ++;
  }
  return counter;

 }
 else if(CATEGORY == 'sat') {
 var list_sat_stmt = snowflake.createStatement({sqlText:`SELECT  sat_table_name, upper(
'CREATE OR REPLACE TABLE EDW.'||sat_table_name
||' ('||
  sub_table.HK||' varchar, '
  ||listagg(sat_column||' '||src_column_data_type,', ')
  ||IFF(s.HASH_DIFF,', HASH_DIFF varchar','')
  ||IFF(s.kafka_dts, ', KAFKA_DTS TIMESTAMP_LTZ', '')
  ||', LOAD_DTS TIMESTAMP_LTZ,  RECORD_SRC varchar'
||');'
) AS DDL_CMD
FROM MTD.mtd_sat s
JOIN ( SELECT sat_table_id, sat_column, src_column_data_type, s_m.version_id
   FROM MTD.mtd_sat_map s_m
JOIN MTD.mtd_src_column src_c
ON s_m.src_column_id = src_c.src_column_id
AND s_m.version_id = src_c.version_id
) AS s_m
ON s.sat_table_id = s_m.sat_table_id
AND s.version_id = s_m.version_id
/*JOIN against HUB/LINK to find out Parent Element - LINK-SAT or HUB-SAT*/
JOIN(
 SELECT hub_table_id, -1 as link_table_id ,version_id, substr(hub_table_name,1,length(hub_table_name)-position('_' IN reverse(hub_table_name)))||'_HK' AS HK
 FROM MTD.MTD_HUB h
 UNION
 SELECT -1 as hub_table_id, link_table_id,version_id, substr(link_table_name,1,length(link_table_name)-position('_' IN reverse(link_table_name)))||'_HK' AS HK
 FROM MTD.MTD_LINK l
) sub_table
ON (sub_table.hub_table_id = s.HUB_TABLE_ID OR sub_table.link_table_id = s.link_table_id)
AND s.version_id = sub_table.version_id
/*Logic to identify Changes in the Scructure of the Table - Record will only get generated if following Logic finds difference between current Table and already existing*/
JOIN (
with
info_schema AS ( SELECT upper(TABLE_NAME) AS TABLE_NAME, upper(COLUMN_NAME) AS COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS col
    JOIN MTD.mtd_sat s
    ON upper(sat_table_name) = upper(col.TABLE_NAME)
    AND s.version_id = (select max(version_id) FROM MTD.MTD_MODEL_CREATION_LOG where approval = 1) 
    JOIN(
     SELECT hub_table_id, -1 as link_table_id ,version_id, upper(substr(hub_table_name,1,length(hub_table_name)-position('_' IN reverse(hub_table_name)))||'_HK') AS HK
     FROM MTD.MTD_HUB h
     UNION
     SELECT -1 as hub_table_id, link_table_id,version_id, upper(substr(link_table_name,1,length(link_table_name)-position('_' IN reverse(link_table_name)))||'_HK') AS HK
     FROM MTD.MTD_LINK l
    ) sub_table
      ON (sub_table.hub_table_id = s.HUB_TABLE_ID OR sub_table.link_table_id = s.link_table_id)
      AND s.version_id = sub_table.version_id     
    where col.COLUMN_NAME NOT IN ('RECORD_SRC','KAFKA_DTS','LOAD_DTS','HASH_DIFF',HK)
                ),
metadata_table AS ( SELECT  upper(sat_table_name) AS TABLE_NAME, upper(sat_column) AS COLUMN_NAME
    FROM MTD.mtd_sat s
    JOIN  MTD.mtd_sat_map s_m
    ON s.sat_table_id = s_m.sat_table_id 
    AND s.version_id = s_m.version_id
    where s.version_id = (select max(version_id) FROM MTD.MTD_MODEL_CREATION_LOG where approval = 1))
    
SELECT DISTINCT table_name  FROM 
(   
  /*Changed Columns*/
  (SELECT * FROM info_schema  
  MINUS
  SELECT * FROM metadata_table)
  
  UNION ALL
  
  /*New Tables/Columns*/
  (SELECT * FROM metadata_table
  MINUS
  SELECT * FROM info_schema)
)
) AS change_check
ON change_check.TABLE_NAME = upper(sat_table_name)
where s.version_id = (select max(version_id) FROM MTD.MTD_MODEL_CREATION_LOG where approval = 1)
GROUP BY s.sat_table_name,sub_table.HK,s.HASH_DIFF,s.kafka_dts;`});

 var tables = list_sat_stmt.execute();
 var counter =0;
 while(tables.next()) {
     var ddl_query = tables.getColumnValue(2);
     var ddl_stmt = snowflake.createStatement({sqlText: ddl_query});
     ddl_stmt.execute();
     counter ++;
 }
 return counter;
 }
 $$;