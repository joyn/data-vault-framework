-- gives you the sql statements for the initial insert into the raw vault tables
select etl_sql from MTD.etl_sql where init_fg = TRUE and version_id = 1;


-- 7 insert stmts in total (that is the output of above query just copy pasted)
-- hubs
INSERT INTO EDW.CUSTOMER_H (CUSTOMER_HK, CUSTOMER_ID, LOAD_DTS, RECORD_SRC) 
SELECT upper(md5(upper(NVL(TRIM( src.record_content:customerId,'"'),'')))) as hash_key_calc, MIN(TRIM(src.record_content:customerId,'"')), to_timestamp_ltz(current_timestamp) as LOAD_DTS,'CUSTOMER' AS RECORD_SRC 
  FROM (SELECT * 
          FROM TST.customer 
         WHERE 1=1  ) src 
 WHERE hash_key_calc NOT IN (SELECT CUSTOMER_HK FROM EDW.CUSTOMER_H) 
 GROUP BY hash_key_calc, LOAD_DTS,  RECORD_SRC;
 
INSERT INTO EDW.CUSTOMER_H (CUSTOMER_HK, CUSTOMER_ID, LOAD_DTS, RECORD_SRC) 
SELECT upper(md5(upper(NVL(TRIM( src.customer_id,'"'),'')))) as hash_key_calc, MIN(TRIM(src.customer_id,'"')), to_timestamp_ltz(current_timestamp) as LOAD_DTS,'SALE' AS RECORD_SRC 
  FROM (SELECT * 
          FROM TST.sale 
         WHERE 1=1  ) src 
 WHERE hash_key_calc NOT IN (SELECT CUSTOMER_HK FROM EDW.CUSTOMER_H) 
 GROUP BY hash_key_calc, LOAD_DTS,  RECORD_SRC;
 
INSERT INTO EDW.SALE_H (SALE_HK, SALE_ID, LOAD_DTS, RECORD_SRC) 
SELECT upper(md5(upper(NVL(TRIM( src.sale_id,'"'),'')))) as hash_key_calc, MIN(TRIM(src.sale_id,'"')), to_timestamp_ltz(current_timestamp) as LOAD_DTS,'SALE' AS RECORD_SRC 
  FROM (SELECT * 
          FROM TST.sale 
         WHERE 1=1  ) src 
 WHERE hash_key_calc NOT IN (SELECT SALE_HK FROM EDW.SALE_H) 
 GROUP BY hash_key_calc, LOAD_DTS,  RECORD_SRC; 

-- link 
INSERT INTO EDW.CUSTOMER_SALE_L (CUSTOMER_SALE_HK , CUSTOMER_HK , SALE_HK , LOAD_DTS, RECORD_SRC) 
SELECT DISTINCT upper(md5(upper(NVL(TRIM( src.customer_id,'"'),'') ||';'|| NVL(TRIM( src.sale_id,'"'),'')))) as hash_key_calc, upper(md5(upper(NVL(TRIM( src.customer_id,'"'),'')))), upper(md5(upper(NVL(TRIM( src.sale_id,'"'),'')))), to_timestamp_ltz(current_timestamp) AS LOAD_DTS,'SALE' AS RECORD_SRC 
  FROM (SELECT * FROM TST.sale WHERE 1=1  )src  
 WHERE hash_key_calc NOT IN ( SELECT CUSTOMER_SALE_HK FROM EDW.CUSTOMER_SALE_L ) 
 GROUP BY hash_key_calc,upper(md5(upper(NVL(TRIM( src.customer_id,'"'),'')))), upper(md5(upper(NVL(TRIM( src.sale_id,'"'),''))));

-- sats 
INSERT INTO EDW.SALE_DETAIL_S (SALE_HK , sale_date, price, HASH_DIFF, LOAD_DTS, RECORD_SRC) 
SELECT DISTINCT upper(md5(upper(NVL(TRIM( src.sale_id,'"'),'')))) as hash_key_calc ,TRIM(src.sale_date,'"'), TRIM(src.total_price,'"'), upper(md5(upper(NVL(TRIM(src.sale_date,'"'),'')||';'||NVL(TRIM(src.total_price,'"'),'')))) as hash_diff_calc , to_timestamp_ltz(current_timestamp) ,'SALE' 
  FROM (SELECT * FROM TST.sale WHERE 1=1  )src  
  LEFT JOIN ( SELECT SALE_HK, hash_diff 
                FROM EDW.SALE_DETAIL_S as tgt 
                JOIN (SELECT SALE_HK as max_hk ,max(LOAD_DTS) as MAX_LOAD_DTS FROM EDW.SALE_DETAIL_S GROUP BY 1) as tgt_max
                  ON tgt.SALE_HK = tgt_max.max_hk AND tgt.LOAD_DTS = tgt_max.MAX_LOAD_DTS ) tgt_out
         ON upper(md5(upper(NVL(TRIM( src.sale_id,'"'),'')))) = SALE_HK 
 WHERE (hash_diff_calc <> hash_diff OR hash_diff is null) AND not (TRIM(src.sale_id,'"') is null);
 
INSERT INTO EDW.CUSTOMER_NAME_S (CUSTOMER_HK , last_name, first_name, HASH_DIFF, KAFKA_DTS, LOAD_DTS, RECORD_SRC) 
SELECT DISTINCT upper(md5(upper(NVL(TRIM( src.record_content:customerId,'"'),'')))) as hash_key_calc ,TRIM(src.record_content:surname,'"'), TRIM(src.record_content:name,'"'), upper(md5(upper(NVL(TRIM(src.record_content:surname,'"'),'')||';'||NVL(TRIM(src.record_content:name,'"'),'')))) as hash_diff_calc , MIN(to_timestamp_ltz(NVL(TRIM(RECORD_METADATA:LogAppendTime,'"'),TRIM(0)))), to_timestamp_ltz(current_timestamp) ,'CUSTOMER' 
  FROM (SELECT * FROM TST.customer WHERE 1=1  )src  
  LEFT JOIN ( SELECT CUSTOMER_HK, hash_diff 
                FROM EDW.CUSTOMER_NAME_S as tgt 
                JOIN (SELECT CUSTOMER_HK as max_hk ,max(KAFKA_DTS) as MAX_LOAD_DTS FROM EDW.CUSTOMER_NAME_S GROUP BY 1) as tgt_max
                  ON tgt.CUSTOMER_HK = tgt_max.max_hk AND tgt.KAFKA_DTS = tgt_max.MAX_LOAD_DTS ) tgt_out
         ON upper(md5(upper(NVL(TRIM( src.record_content:customerId,'"'),'')))) = CUSTOMER_HK 
 WHERE (hash_diff_calc <> hash_diff OR hash_diff is null) AND not (TRIM(src.record_content:customerId,'"') is null) 
 GROUP BY upper(md5(upper(NVL(TRIM( src.record_content:customerId,'"'),'')))),TRIM(src.record_content:surname,'"'), TRIM(src.record_content:name,'"'), hash_diff_calc , to_timestamp_ltz(current_timestamp) ,'CUSTOMER';

INSERT INTO EDW.CUSTOMER_ADDRESS_S (CUSTOMER_HK , address, HASH_DIFF, KAFKA_DTS, LOAD_DTS, RECORD_SRC) 
SELECT DISTINCT upper(md5(upper(NVL(TRIM( src.record_content:customerId,'"'),'')))) as hash_key_calc ,src.record_content:address, upper(md5(upper(NVL(TRIM(src.record_content:address,'"'),'')))) as hash_diff_calc , MIN(to_timestamp_ltz(NVL(TRIM(RECORD_METADATA:LogAppendTime,'"'),TRIM(0)))), to_timestamp_ltz(current_timestamp) ,'CUSTOMER' 
  FROM (SELECT * FROM TST.customer WHERE 1=1  )src  
  LEFT JOIN ( SELECT CUSTOMER_HK, hash_diff 
                FROM EDW.CUSTOMER_ADDRESS_S as tgt 
                JOIN (SELECT CUSTOMER_HK as max_hk ,max(KAFKA_DTS) as MAX_LOAD_DTS FROM EDW.CUSTOMER_ADDRESS_S GROUP BY 1) as tgt_max
                  ON tgt.CUSTOMER_HK = tgt_max.max_hk AND tgt.KAFKA_DTS = tgt_max.MAX_LOAD_DTS ) tgt_out
         ON upper(md5(upper(NVL(TRIM( src.record_content:customerId,'"'),'')))) = CUSTOMER_HK 
 WHERE (hash_diff_calc <> hash_diff OR hash_diff is null) AND not (TRIM(src.record_content:customerId,'"') is null) 
 GROUP BY upper(md5(upper(NVL(TRIM( src.record_content:customerId,'"'),'')))),src.record_content:address, hash_diff_calc , to_timestamp_ltz(current_timestamp) ,'CUSTOMER';