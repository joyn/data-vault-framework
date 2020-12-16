-- stream and task creation (streams if they dont exist, tasks are replaced per default)
-- replace the warehouse in the following function call!
CALL MTD.PROC_ETL_STREAM_TASK_CREATE('MY_PREFERRED_WAREHOUSE', 5);

-- resume all tasks per source table
CALL mtd.task_dependency_handler_source_table ('customer', 'resume');
CALL mtd.task_dependency_handler_source_table ('sale', 'resume');

/*
-- note: to suspend everything again just run:
CALL mtd.task_dependency_handler_source_table ('customer', 'suspend');
CALL mtd.task_dependency_handler_source_table ('sale', 'suspend');
*/