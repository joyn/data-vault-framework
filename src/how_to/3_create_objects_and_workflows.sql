-- based on metadata ...
-- create new raw vault tables in edw
CALL MTD.PROC_CREATE_TABLES('hub');
CALL MTD.PROC_CREATE_TABLES('link');
CALL MTD.PROC_CREATE_TABLES('sat');

-- initial insert scripts
CALL MTD.PROC_ETL_HUB('true');
CALL MTD.PROC_ETL_LINK('true');
CALL MTD.PROC_ETL_SAT('true');

-- retrieve the load SQL statements you have just created
select etl_sql from MTD.etl_sql where init_fg = TRUE and version_id = 1;

-- insert scripts for continous load via stream
CALL MTD.PROC_ETL_HUB('false');
CALL MTD.PROC_ETL_LINK('false');
CALL MTD.PROC_ETL_SAT('false');

-- retrieve the load SQL statements you have just created
select etl_sql from MTD.etl_sql where init_fg = FALSE and version_id = 1;