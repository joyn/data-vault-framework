-- basic function to resume or suspend (a tree of) tasks automatically
CREATE OR REPLACE PROCEDURE mtd.task_dependency_handler (task_name STRING, task_schema STRING, task_action STRING)
  RETURNS varchar not null
  LANGUAGE javascript
  EXECUTE AS CALLER
  AS
  $$
    var sql_show_tasks = `SHOW TASKS;`
    var result_show_tasks = snowflake.createStatement({sqlText: sql_show_tasks}).execute();
    
    var sql_task_dependencies = `
    WITH recursive task_dependencies (task_level, task_name, task_name_predecessor, task_state) AS (
      SELECT 1 as task_level, ("database_name" || '.' || "schema_name" || '.' || "name") as task_name, "predecessors" as task_name_predecessor, "state" as task_state 
        FROM table(result_scan(last_query_id()))
       WHERE task_name = CURRENT_DATABASE() || '.' || '` + TASK_SCHEMA + `' || '.' || '` + TASK_NAME + `'

       UNION ALL

      SELECT task_level + 1 as task_level, (th1."database_name" || '.' || th1."schema_name" || '.' || th1."name") as task_name, th1."predecessors" as task_name_predecessor, th1."state" as task_state
        FROM table(result_scan(last_query_id())) th1
             JOIN task_dependencies th2 ON (th1."database_name" || '.' || th1."schema_name" || '.' || th1."name") = th2.task_name_predecessor
    )
    SELECT td.task_level, td.task_name, td.task_name_predecessor, td.task_state 
      FROM task_dependencies td
     WHERE 1=1
       AND td.task_name_predecessor IS NULL;`
    try {
      var result_task_dependencies = snowflake.createStatement({sqlText: sql_task_dependencies}).execute();
      
      if (result_task_dependencies.next()) {
        if (TASK_ACTION.toLowerCase() == 'suspend') {
          sql_task_action = `ALTER TASK ` + result_task_dependencies.getColumnValue(2) + ` suspend;`;
          var result_task_action = snowflake.createStatement({sqlText: sql_task_action}).execute();
          var output = "Root Task suspended"
        }
        else if (TASK_ACTION.toLowerCase() == 'resume') {
          sql_task_action = `SELECT SYSTEM$TASK_DEPENDENTS_ENABLE( '` + result_task_dependencies.getColumnValue(2) + `' );`;
          var result_task_action = snowflake.createStatement({sqlText: sql_task_action}).execute();
          var output = "Task(s) resumed";
        }
        else {
          var output = "Unsupported task action specified, must be either suspend or resume";
        }
      } else {
          var output = "No result set found. Please check if the given task exists";
      }
    } 
    catch (err)  {
      output =  "Failed: Code: " + err.code + "\n  State: " + err.state;
      output += "\n  Message: " + err.message;
      output += "\nStack Trace:\n" + err.stackTraceTxt;
    }
    return output;
  $$
  ;
  
  
-- extension of task_dependency_handler by using a souce table, iterating over raw vault target tables and finding all tasks that operate on them
CREATE OR REPLACE PROCEDURE mtd.task_dependency_handler_source_table (source_table_name STRING, task_action STRING)
    RETURNS varchar not null
    LANGUAGE javascript
    EXECUTE AS CALLER
    AS
    $$
    var output = "";
    
    // functionality limited to schema MTD currently
    var sql_use_schema = `USE SCHEMA MTD;`;
    var result_use_schema = snowflake.createStatement({sqlText: sql_use_schema}).execute();
    
    var sql_show_tasks = `SHOW TASKS LIKE 'TASK_` + SOURCE_TABLE_NAME + `%_TO_%';`;
    var result_show_tasks = snowflake.createStatement({sqlText: sql_show_tasks}).execute();
    
    var sql_get_task_names = `SELECT "name"::STRING as task_name FROM table(result_scan(last_query_id()));`;
    var result_get_task_names = snowflake.createStatement({sqlText: sql_get_task_names}).execute();    

    while (result_get_task_names.next()) { 
      task_name = result_get_task_names.getColumnValue(1);
      var stmt_call_procedure = snowflake.createStatement
      ({ 
                                sqlText: 'CALL mtd.task_dependency_handler (:1, :2, :3);',
                                binds: [task_name, 'MTD', TASK_ACTION]
      });
      var result_call_procedure = stmt_call_procedure.execute();
      output += "\nTask " + task_name + ": " + TASK_ACTION;
    }       
    return output;
    $$
    ; 