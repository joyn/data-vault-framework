CREATE or replace PROCEDURE mtd.proc_etl_stream_task_create()
returns string
language javascript
as
 $$
var main = snowflake.createStatement({sqlText:`
WITH es AS (
SELECT 'TASK_' || es.SRC_TABLE_NAME|| '_TO_' || es.TGT_TABLE_NAME as TASK, row_number() over (partition by es.TGT_TABLE_NAME order by version_id,SRC_TABLE_NAME,type) as row_num,
es.* from mtd.etl_sql es where INIT_FG = 0)
SELECT 
upper('CREATE STREAM IF NOT EXISTS STREAM_' || es.SRC_TABLE_NAME|| '_TO_' || es.TGT_TABLE_NAME || ' ON TABLE ' || src_t.SRC_TABLE_DATABASE ||'.'||  src_t.SRC_TABLE_SCHEMA || '.' || es.SRC_TABLE_NAME||' APPEND_ONLY = TRUE;') AS STREAM_DDL,
'CREATE OR REPLACE TASK ' || es.TASK || ' WAREHOUSE = MICRO_ETL ' || IFF(es2.task is not null, ' AFTER ' || es2.task || ' AS ',' schedule = \\\'10 minute\\\' AS ')|| es.etl_sql||';' AS TASK_DDL
FROM es
JOIN MTD.MTD_SRC_TABLE src_t
ON es.SRC_TABLE_NAME = upper(src_t.SRC_TABLE_NAME)
AND es.version_id = src_t.version_id
LEFT JOIN es as es2
ON es.TGT_TABLE_NAME = es2.TGT_TABLE_NAME
AND es.row_num = es2.row_num+1
WHERE es.version_id = (SELECT MAX(version_id) FROM MTD.MTD_MODEL_CREATION_LOG WHERE approval = 1)
ORDER BY task_ddl
;`});

var main_val = main.execute();
var counter =0;  
while(main_val.next()) {
  var STREAM_DDL = main_val.getColumnValue(1);
  var TASK_DDL = main_val.getColumnValue(2);

  var create_stream = snowflake.createStatement({sqlText:STREAM_DDL});
  var create_stream_exec = create_stream.execute();
  
  var create_task = snowflake.createStatement({sqlText:TASK_DDL});
  var create_task_exec = create_task.execute(); 
  counter ++;
}
return counter;
$$;